from functools import lru_cache
from pprint import pprint
from requests.models import Response
import json
import requests
import textwrap
import time


host: str = 'http://localhost:8998'
session_kind: dict = {'kind': 'spark'}
headers: dict = {'Content-Type': 'application/json'}


def _cache_local(data: dict) -> None:
    with open('local.json', 'w', encoding='utf-8') as f:
        json.dump(data, f)


def _delete_cache() -> None:
    with open('local.json', 'w', encoding='utf-8') as f:
        f.truncate()


def _create_session() -> str:
    response: Response = requests.post(
        url=f"{host}/sessions",
        data=json.dumps(session_kind),
        headers=headers
    )
    session: dict = {
        'url': host + response.headers['location']
    }
    _cache_local(session)
    return session['url']


def delete_session() -> None:
    with open('local.json', 'r') as f:
        try:
            session: dict = json.load(f)
            requests.delete(data['url'])
        except Exception:
            pass

    _delete_cache()


@lru_cache(maxsize=32)
def retrieve_session() -> str:
    session_url: str = ""
    try:
        with open('local.json', 'r') as f:
            data: dict = json.load(f)
            session_url = data['url']
    except Exception:
            session_url = _create_session()
    return session_url


def execute_code(code_to_execute: dict, **kwargs) -> Response:
    session_url: str = retrieve_session()
    statements_url: str = f"{session_url}/statements"
    response: Response = requests.post(
        statements_url,
        data=json.dumps(code_to_execute),
        headers=headers,
        **kwargs
    )
    return requests.get(statements_url, headers=headers)
