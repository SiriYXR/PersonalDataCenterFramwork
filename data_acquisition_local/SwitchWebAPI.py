import requests
import json
import base64
import hashlib
import re
import sys
import os

def NS_GetSessionToken(client_id,ua):
    '''Logs in to a Nintendo Account and returns a session_token.'''
    session = requests.Session()

    auth_code_verifier = base64.urlsafe_b64encode(os.urandom(32))
    auth_cv_hash = hashlib.sha256()
    auth_cv_hash.update(auth_code_verifier.replace(b"=", b""))
    auth_code_challenge = base64.urlsafe_b64encode(auth_cv_hash.digest())

    app_head = {
        'Host':                      'accounts.nintendo.com',
        'Connection':                'keep-alive',
        'Cache-Control':             'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent':                'Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Mobile Safari/537.36',
        'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8n',
        'DNT':                       '1',
        'Accept-Encoding':           'gzip,deflate,br',
    }
    body = {
        'state':                               '',
        'redirect_uri':                        'npf{}://auth'.format(client_id),
        'client_id':                           client_id,
        'scope':                               'openid user user.mii user.email user.links[].id',
        'response_type':                       'session_token_code',
        'session_token_code_challenge':        auth_code_challenge.replace(b"=", b""),
        'session_token_code_challenge_method': 'S256',
        'theme':                               'login_form'
    }

    url = 'https://accounts.nintendo.com/connect/1.0.0/authorize'
    r = session.get(url, headers=app_head, params=body)

    post_login = r.history[0].url

    print("\nMake sure you have fully read the \"Cookie generation\" section of the readme before proceeding. To manually input a cookie instead, enter \"skip\" at the prompt below.")
    print("\nNavigate to this URL in your browser:")
    print(post_login)
    print("Log in, right click the \"Select this account\" button, copy the link address, and paste it below:")
    while True:
        try:
            use_account_url = input("")
            if use_account_url == "skip":
                return "skip"
            session_token_code = re.search('de=(.*)&', use_account_url)

            # get session tocken
            app_head = {
                'User-Agent':      ua,
                'Accept-Language': 'en-US',
                'Accept':          'application/json',
                'Content-Type':    'application/x-www-form-urlencoded',
                'Host':            'accounts.nintendo.com',
                'Connection':      'Keep-Alive',
                'Accept-Encoding': 'gzip'
            }

            body = {
                'client_id':                   client_id,
                'session_token_code':          session_token_code.group(1),
                'session_token_code_verifier': auth_code_verifier.replace(b"=", b"")
            }

            url = 'https://accounts.nintendo.com/connect/1.0.0/api/session_token'

            r = session.post(url, headers=app_head, data=body)
            session_token = json.loads(r.text)["session_token"]

            return session_token
        except KeyboardInterrupt:
            print("\nBye!")
            sys.exit(1)
        except AttributeError:
            print("Malformed URL. Please try again, or press Ctrl+C to exit.")
            print("URL:", end=' ')
        except KeyError:  # session_token not found
            print(
                "\nThe URL has expired. Please log out and back into your Nintendo Account and try again.")
            sys.exit(1)


def NS_GetAccessToken(client_id,session_token):
    session = requests.Session()

    body = '{"client_id":"' + client_id + '","session_token":"' + session_token + \
        '","grant_type":"urn:ietf:params:oauth:grant-type:jwt-bearer-session-token"}'
    url = 'https://accounts.nintendo.com/connect/1.0.0/api/token'

    r = session.post(
        url, headers={'Content-Type': 'application/json'}, data=body)
    access_token = json.loads(r.text)

    return access_token
 
def NS_GetPlayHistory(access_token,ua):
    session = requests.Session()

    url = 'https://mypage-api.entry.nintendo.co.jp/api/v1/users/me/play_histories'
    header = {
        'Authorization': access_token['token_type'] + ' ' + access_token['access_token'],
        'User-Agent': ua,
    }
    r = session.get(url, headers=header)
    history=json.loads(r.text)

    return history

if __name__ =="__main__":

    client_id = "5c38e31cd085304b"
    ua = 'com.nintendo.znej/1.13.0 (Android/7.1.2)'

    print(NS_GetSessionToken(client_id,ua))