from flask import Flask, request, jsonify, Response, stream_with_context
import requests
import json
import base64
import time
from deepseek_pow import Challenge, registry

app = Flask(__name__)
app.json.sort_keys = False

DEV_NAME = "Tofazzal Hossain"

AUTH_TOKEN = "mSFjPsANFM5BS0RPYcweg+S3WuG12Y5TpkXLuSMxLe5M1bHwDT/DKZbZohMag2si"
DS_SESSION = "8c5774e6b37c4e69bc1dfc1f29e5e093"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0',
    'Accept': '*/*',
    'x-client-bundle-id': 'com.deepseek.chat',
    'x-client-platform': 'web',
    'x-client-version': '2.3.0',
    'x-client-locale': 'en_US',
    'x-client-timezone-offset': '21600',
    'authorization': f'Bearer {AUTH_TOKEN}',
    'content-type': 'application/json',
    'Origin': 'https://chat.deepseek.com',
    'Referer': 'https://chat.deepseek.com/',
    'Cookie': f'ds_session_id={DS_SESSION}'
}


def solve_pow(challenge_data):
    challenge = Challenge(
        algorithm=challenge_data['algorithm'],
        challenge=challenge_data['challenge'],
        salt=challenge_data['salt'],
        difficulty=challenge_data['difficulty'],
        expire_at=challenge_data['expire_at'],
        signature=challenge_data['signature'],
    )
    solution = registry.get(challenge.algorithm).solve(challenge)

    payload = {
        'algorithm': solution.algorithm,
        'challenge': solution.challenge,
        'salt': solution.salt,
        'answer': int(solution.answer),
        'signature': solution.signature,
        'target_path': challenge_data.get('target_path', '')
    }
    return base64.b64encode(json.dumps(payload).encode()).decode()


def parse_sse(text):
    content = ""
    for line in text.split('\n'):
        if line.startswith('data: '):
            try:
                data = json.loads(line[6:])
                v = data.get('v')
                p = data.get('p', '')
                o = data.get('o', '')
                if isinstance(v, str) and v not in ['FINISHED', 'WIP', '']:
                    if p == 'response/fragments/-1/content' or o == 'APPEND':
                        content += v
                    elif p == '' and o == '':
                        content += v
                elif isinstance(v, dict) and 'response' in v:
                    frags = v['response'].get('fragments', [])
                    for f in frags:
                        c = f.get('content', '')
                        if c and not content:
                            content = c
            except (json.JSONDecodeError, KeyError):
                pass
    return content.strip()


def stream_sse_response(session, chat_session_id, prompt, pow_b64):
    """Generator function to stream SSE responses"""
    try:
        with session.post('https://chat.deepseek.com/api/v0/chat/completion',
            json={
                "chat_session_id": chat_session_id,
                "parent_message_id": None,
                "model_type": None,
                "prompt": prompt,
                "ref_file_ids": [],
                "thinking_enabled": False,
                "search_enabled": False,
                "action": None,
                "preempt": False
            },
            headers={'x-ds-pow-response': pow_b64},
            timeout=60,
            stream=True) as response:
            
            for line in response.iter_lines(decode_unicode=True):
                if line and line.startswith('data: '):
                    try:
                        data = json.loads(line[6:])
                        v = data.get('v')
                        p = data.get('p', '')
                        o = data.get('o', '')
                        
                        # Extract content chunks
                        content_chunk = None
                        if isinstance(v, str) and v not in ['FINISHED', 'WIP', '']:
                            if p == 'response/fragments/-1/content' or o == 'APPEND':
                                content_chunk = v
                            elif p == '' and o == '':
                                content_chunk = v
                        elif isinstance(v, dict) and 'response' in v:
                            frags = v['response'].get('fragments', [])
                            for f in frags:
                                c = f.get('content', '')
                                if c:
                                    content_chunk = c
                                    break
                        
                        if content_chunk:
                            # Send as SSE format
                            yield f"data: {json.dumps({'chunk': content_chunk})}\n\n"
                        
                        # Check for completion
                        if isinstance(v, str) and v == 'FINISHED':
                            yield f"data: {json.dumps({'done': True})}\n\n"
                            break
                            
                    except (json.JSONDecodeError, KeyError) as e:
                        continue
                        
    except Exception as e:
        yield f"data: {json.dumps({'error': str(e)})}\n\n"


@app.route('/api/chat', methods=['GET'])
def chat():
    prompt = request.args.get('prompt')

    if not prompt:
        return jsonify({'success': False, 'developer': DEV_NAME, 'message': 'prompt required'})

    try:
        s = requests.Session()
        s.headers.update(HEADERS)

        session_resp = s.post('https://chat.deepseek.com/api/v0/chat_session/create', timeout=15)
        chat_session_id = session_resp.json()['data']['biz_data']['chat_session']['id']

        pow_resp = s.post('https://chat.deepseek.com/api/v0/chat/create_pow_challenge',
            json={"target_path": "/api/v0/chat/completion"}, timeout=15)
        cd = pow_resp.json()['data']['biz_data']['challenge']

        pow_b64 = solve_pow(cd)

        chat_resp = s.post('https://chat.deepseek.com/api/v0/chat/completion',
            json={
                "chat_session_id": chat_session_id,
                "parent_message_id": None,
                "model_type": None,
                "prompt": prompt,
                "ref_file_ids": [],
                "thinking_enabled": False,
                "search_enabled": False,
                "action": None,
                "preempt": False
            },
            headers={'x-ds-pow-response': pow_b64},
            timeout=60)

        reply = parse_sse(chat_resp.text)

        try:
            s.post('https://chat.deepseek.com/api/v0/chat_session/delete',
                json={"chat_session_id": chat_session_id}, timeout=10)
        except Exception:
            pass

        return jsonify({'success': True, 'developer': DEV_NAME, 'data': {'reply': reply}})

    except Exception as e:
        return jsonify({'success': False, 'developer': DEV_NAME, 'message': str(e)})


@app.route('/stream', methods=['GET'])
def stream_chat():
    """Streaming endpoint that returns chunks as Server-Sent Events"""
    prompt = request.args.get('prompt')

    if not prompt:
        return jsonify({'success': False, 'developer': DEV_NAME, 'message': 'prompt required'})

    def generate():
        try:
            s = requests.Session()
            s.headers.update(HEADERS)

            # Create chat session
            session_resp = s.post('https://chat.deepseek.com/api/v0/chat_session/create', timeout=15)
            chat_session_id = session_resp.json()['data']['biz_data']['chat_session']['id']

            # Get POW challenge
            pow_resp = s.post('https://chat.deepseek.com/api/v0/chat/create_pow_challenge',
                json={"target_path": "/api/v0/chat/completion"}, timeout=15)
            cd = pow_resp.json()['data']['biz_data']['challenge']

            pow_b64 = solve_pow(cd)

            # Send initial message
            yield f"data: {json.dumps({'status': 'connected'})}\n\n"

            # Stream the response
            for chunk in stream_sse_response(s, chat_session_id, prompt, pow_b64):
                yield chunk

            # Clean up
            try:
                s.post('https://chat.deepseek.com/api/v0/chat_session/delete',
                    json={"chat_session_id": chat_session_id}, timeout=10)
            except Exception:
                pass

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(stream_with_context(generate()), 
                   mimetype='text/event-stream',
                   headers={
                       'Cache-Control': 'no-cache',
                       'X-Accel-Buffering': 'no'
                   })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
