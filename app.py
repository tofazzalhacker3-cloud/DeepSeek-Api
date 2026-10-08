from flask import Flask, request, jsonify, Response, stream_with_context
import requests
import json
import base64
import os
import time
from deepseek_pow import Challenge, registry

app = Flask(__name__)
app.json.sort_keys = False

DEV_NAME = "Tofazzal Hossain"

AUTH_TOKEN = os.environ.get('DS_AUTH_TOKEN',
    "SOOHr3AV/0eRBTBVuKI39Imy9fPwlfBvd57SWnumI4rSXQ+cWFqCcTDyJv+N1ui6")
DS_SESSION = os.environ.get('DS_SESSION_ID', "b728aaefe319469e991d5e63eeae611c")
DEVICE_ID = os.environ.get('DS_DEVICE_ID', "f6e63487-f0fb-4325-8862-bf0d3a78b76a")
HIF_LEIM = os.environ.get('DS_HIF_LEIM',
    "Qcx6hYF2VFZDKU8elo6+gWMjhdehyQ9qMeyOTq7d0ZM+iNivuLOsXkY=.T2l2PwEJq17Fh277")

COOKIES = {
    'smidV2': '202609022215168269fadf3671f41f300201b3cab995810096c051b8b31e7f0',
    'aws-waf-token': os.environ.get('DS_WAF_TOKEN',
        '6b70eacf-1737-4ac4-8853-4b7b6c6d0256:HgoAY+ZqByrpAAAA:OfN/ynmLWkokBMUOPpH/bUMktLQBuKoLWe6bfO0eM336SYaQK3nTZIm9tQKHcYVGPCwaL/6bcqzIjy1ZouDJFS6URL9EvcMT60AyDD2IBy/bcGtqtbizAlbkoz7M9YbzvNKpC8T1yO1kaLwIYFmPwjycZvS1MeJHe8SxleBhdU0mxYtGDzwqrHCOzr8UnbMzH07eJvd06YU66/JCHVNhtsVAqZGNRqJK4aL7c79i1UEhB1ooIDougodScWHNyRMtj84+havqSE2nc6SyKZAhd0pROpizHtnv0zW4aQgZY973N8BQ8Ap19sqM6drupnwmN7V07PP4OFXqt8uBEyugU9wMRQb316W2o+fjEe5n6+3bqK2DIWRG8133x4U7tPzxzA=='),
    '.thumbcache_6b2e5483f9d858d7c661c5e276b6a6ae':
        'jn6E6Q/jJnOUQUQ6aiQWdfMQQ0OuB4JWtXMqpVbeeaiQ5Eq+qvPcVGJhsbD1G4XNGJsUZVZEnsIxgnAiQB3ViQ==',
    'ds_session_id': DS_SESSION,
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'x-client-bundle-id': 'com.deepseek.chat',
    'x-client-platform': 'web',
    'x-client-version': '2.5.0',
    'x-client-locale': 'en_US',
    'x-client-timezone-offset': '21600',
    'x-device-id': DEVICE_ID,
    'x-hif-leim': HIF_LEIM,
    'dnt': '1',
    'authorization': f'Bearer {AUTH_TOKEN}',
    'content-type': 'application/json',
    'Origin': 'https://chat.deepseek.com',
    'Referer': 'https://chat.deepseek.com/',
}


def api_json(resp):
    try:
        body = resp.json()
    except ValueError:
        raise RuntimeError(f'HTTP {resp.status_code}: {resp.text[:200]}')
    if not isinstance(body, dict):
        raise RuntimeError(f'unexpected response: {str(body)[:200]}')
    if body.get('code') not in (0, None) or body.get('data') is None:
        raise RuntimeError(f"DeepSeek error {body.get('code')}: {body.get('msg') or 'no data'}")
    return body


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
            
            if 'application/json' in response.headers.get('Content-Type', ''):
                body = response.text
                try:
                    err = json.loads(body)
                    raise RuntimeError(f"DeepSeek error {err.get('code')}: {err.get('msg') or body[:200]}")
                except ValueError:
                    raise RuntimeError(f'HTTP {response.status_code}: {body[:200]}')

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
        s.cookies.update(COOKIES)

        session_resp = s.post('https://chat.deepseek.com/api/v0/chat_session/create', timeout=15)
        chat_session_id = api_json(session_resp)['data']['biz_data']['chat_session']['id']

        pow_resp = s.post('https://chat.deepseek.com/api/v0/chat/create_pow_challenge',
            json={"target_path": "/api/v0/chat/completion"}, timeout=15)
        cd = api_json(pow_resp)['data']['biz_data']['challenge']

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

        if 'application/json' in chat_resp.headers.get('Content-Type', ''):
            api_json(chat_resp)

        reply = parse_sse(chat_resp.text)
        if not reply:
            raise RuntimeError(f'empty reply (HTTP {chat_resp.status_code}): {chat_resp.text[:200]}')

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
            s.cookies.update(COOKIES)

            # Create chat session
            session_resp = s.post('https://chat.deepseek.com/api/v0/chat_session/create', timeout=15)
            chat_session_id = api_json(session_resp)['data']['biz_data']['chat_session']['id']

            # Get POW challenge
            pow_resp = s.post('https://chat.deepseek.com/api/v0/chat/create_pow_challenge',
                json={"target_path": "/api/v0/chat/completion"}, timeout=15)
            cd = api_json(pow_resp)['data']['biz_data']['challenge']

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
