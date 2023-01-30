import requests
import json

def Steam_GetOwnedGames(tocken,steamid):
    url = "http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=%s&steamid=%s&format=json&include_appinfo=1&include_played_free_games=1" % (tocken,steamid)

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)

    res=json.loads(response.text)

    return res

def Steam_GetRecentlyPlayedGames(tocken,steamid):
    url = "http://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key=%s&steamid=%s&format=json" % (tocken,steamid)

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)

    res=json.loads(response.text)

    return res