"""
Auth Utils Module

Implements dynamic x-mas header generation for FotMob API.
Replicates the JavaScript signature logic found in FotMob's frontend.
"""

import hashlib
import json
import base64
import time
import logging
import requests
import re

logger = logging.getLogger(__name__)

# The secret key is the lyrics to "Three Lions" (Football's Coming Home)
# as found in FotMob's minified JavaScript.
SECRET_KEY = """[Spoken Intro: Alan Hansen & Trevor Brooking]
I think it's bad news for the English game
We're not creative enough, and we're not positive enough

[Refrain: Ian Broudie & Jimmy Hill]
It's coming home, it's coming home, it's coming
Football's coming home (We'll go on getting bad results)
It's coming home, it's coming home, it's coming
Football's coming home
It's coming home, it's coming home, it's coming
Football's coming home
It's coming home, it's coming home, it's coming
Football's coming home

[Verse 1: Frank Skinner]
Everyone seems to know the score, they've seen it all before
They just know, they're so sure
That England's gonna throw it away, gonna blow it away
But I know they can play, 'cause I remember

[Chorus: All]
Three lions on a shirt
Jules Rimet still gleaming
Thirty years of hurt
Never stopped me dreaming

[Verse 2: David Baddiel]
So many jokes, so many sneers
But all those "Oh, so near"s wear you down through the years
But I still see that tackle by Moore and when Lineker scored
Bobby belting the ball, and Nobby dancing

[Chorus: All]
Three lions on a shirt
Jules Rimet still gleaming
Thirty years of hurt
Never stopped me dreaming

[Bridge]
England have done it, in the last minute of extra time!
What a save, Gordon Banks!
Good old England, England that couldn't play football!
England have got it in the bag!
I know that was then, but it could be again

[Refrain: Ian Broudie]
It's coming home, it's coming
Football's coming home
It's coming home, it's coming home, it's coming
Football's coming home
(England have done it!)
It's coming home, it's coming home, it's coming
Football's coming home
It's coming home, it's coming home, it's coming
Football's coming home
[Chorus: All]
(It's coming home) Three lions on a shirt
(It's coming home, it's coming) Jules Rimet still gleaming
(Football's coming home
It's coming home) Thirty years of hurt
(It's coming home, it's coming) Never stopped me dreaming
(Football's coming home
It's coming home) Three lions on a shirt
(It's coming home, it's coming) Jules Rimet still gleaming
(Football's coming home
It's coming home) Thirty years of hurt
(It's coming home, it's coming) Never stopped me dreaming
(Football's coming home
It's coming home) Three lions on a shirt
(It's coming home, it's coming) Jules Rimet still gleaming
(Football's coming home
It's coming home) Thirty years of hurt
(It's coming home, it's coming) Never stopped me dreaming
(Football's coming home)"""

# Using a global state to keep track of auth info across calls
_SESSION_AUTH = {
    "client_version": None,
    "cookies": {},
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

def get_live_client_version() -> str:
    """Fetch the latest client_version (buildId) from the FotMob homepage HTML."""
    headers = {
        'User-Agent': _SESSION_AUTH["user_agent"],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    try:
        response = requests.get('https://www.fotmob.com/', headers=headers, timeout=10)
        html = response.text
        build_id_match = re.search(r'"buildId":"([^"]+)"', html)
        if build_id_match:
            build_id = build_id_match.group(1)
            logger.info(f"Loaded live client version: production:{build_id}")
            return f"production:{build_id}"
            
        hex_hashes = re.findall(r'[0-9a-f]{40}', html)
        if hex_hashes:
            logger.info(f"Fallback to hex hash client version: production:{hex_hashes[0]}")
            return f"production:{hex_hashes[0]}"
    except Exception as e:
        logger.error(f"Failed to fetch live client version: {e}")
        
    logger.warning("Falling back to hardcoded client_version")
    return "production:374eada7701109a1cac357a05790c4fd2ac94e1a"

def set_auth_info(auth_info: dict):
    """Set the captured auth info (x_mas for version, and cookies)."""
    global _SESSION_AUTH
    if not auth_info:
        return
    
    # Extract version from X-MAS token if possible
    x_mas = auth_info.get('x_mas')
    if x_mas:
        try:
            import base64
            import json
            payload = json.loads(base64.b64decode(x_mas).decode('utf-8'))
            # In the new format, the client version is in body -> foo
            version = payload.get('body', {}).get('foo')
            if version:
                _SESSION_AUTH["client_version"] = version
                logger.info(f"Updated client version to: {version}")
        except Exception as e:
            logger.warning(f"Failed to extract version from x-mas: {e}")
            
    _SESSION_AUTH["cookies"] = auth_info.get('cookies') or {}
    _SESSION_AUTH["user_agent"] = auth_info.get('user_agent') or _SESSION_AUTH["user_agent"]
    logger.info(f"Updated session cookies ({len(_SESSION_AUTH['cookies'])} found) and user-agent")

def generate_x_mas_header(url: str) -> str:
    """
    Generate the x-mas authentication header for a given URL.
    
    Args:
        url: The relative API URL (e.g., '/api/data/matchDetails?matchId=4830678')
        
    Returns:
        Base64-encoded x-mas header string
    """
    timestamp = int(time.time() * 1000)
    
    if _SESSION_AUTH["client_version"] is None:
        _SESSION_AUTH["client_version"] = get_live_client_version()
        
    client_version = _SESSION_AUTH["client_version"]
    
    body = {
        "url": url,
        "code": timestamp,
        "foo": client_version
    }
    
    # Replicate JavaScript's JSON.stringify(body) which has no whitespace
    body_str = json.dumps(body, separators=(',', ':'))
    
    # Signature: MD5(json_body + secret_key)
    signature_input = body_str + SECRET_KEY
    signature = hashlib.md5(signature_input.encode('utf-8')).hexdigest().upper()
    
    # Final payload: {body, signature}
    final_payload = {
        "body": body,
        "signature": signature
    }
    
    payload_str = json.dumps(final_payload, separators=(',', ':'))
    
    # Base64 encode the final JSON string
    x_mas = base64.b64encode(payload_str.encode('utf-8')).decode('utf-8')
    
    return x_mas

def get_auth_headers(url_path: str) -> dict:
    """
    Get all required headers for an API call, including dynamic x-mas.
    """
    x_mas = generate_x_mas_header(url_path)
    
    # Basic headers that seem to be consistent
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.fotmob.com/',
        'user-agent': _SESSION_AUTH["user_agent"],
        'x-mas': x_mas,
    }
    
    # Add cookies
    if _SESSION_AUTH["cookies"]:
        cookie_parts = [f"{k}={v}" for k, v in _SESSION_AUTH["cookies"].items()]
        headers['cookie'] = "; ".join(cookie_parts)
        
    return headers
