"""
Cognito OAuth callback handler
"""
from flask import Blueprint, request, redirect, session, jsonify
import requests
import json
import base64

cognito_callback_bp = Blueprint('cognito_callback', __name__)

# Cognito configuration
USER_POOL_DOMAIN = 'https://us-east-1k9zw8nznd.auth.us-east-1.amazoncognito.com'
CLIENT_ID = '1da4nl8rlu0u3ec9n0hqke210c'
REDIRECT_URI = 'https://d225ar6c86586s.cloudfront.net/callback'

@cognito_callback_bp.route('/callback')
def cognito_callback():
    """Handle Cognito OAuth callback."""

    # Get the authorization code from query parameters
    code = request.args.get('code')

    if not code:
        return jsonify({'error': 'No authorization code received'}), 400

    try:
        # Exchange code for tokens
        token_url = f'{USER_POOL_DOMAIN}/oauth2/token'

        token_data = {
            'grant_type': 'authorization_code',
            'client_id': CLIENT_ID,
            'code': code,
            'redirect_uri': REDIRECT_URI
        }

        # Request tokens from Cognito with proper headers
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        token_response = requests.post(token_url, data=token_data, headers=headers)

        if token_response.status_code == 200:
            tokens = token_response.json()

            # Store tokens in session
            session['id_token'] = tokens.get('id_token')
            session['access_token'] = tokens.get('access_token')
            session['refresh_token'] = tokens.get('refresh_token')

            # Parse ID token to get user info (JWT)
            id_token = tokens.get('id_token', '')
            if id_token:
                # Decode JWT payload (middle part)
                parts = id_token.split('.')
                if len(parts) > 1:
                    # Add padding if needed
                    payload = parts[1]
                    payload += '=' * (4 - len(payload) % 4)

                    try:
                        user_info = json.loads(base64.b64decode(payload))
                        session['user_email'] = user_info.get('email')
                        session['user_id'] = user_info.get('sub')
                        session['authenticated'] = True
                    except:
                        pass

            # Redirect to success page with welcome message
            return f'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Login Successful</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        margin: 0;
                    }}
                    .container {{
                        background: white;
                        border-radius: 20px;
                        padding: 40px;
                        text-align: center;
                        box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                        max-width: 500px;
                    }}
                    .success {{
                        color: #155724;
                        font-size: 24px;
                        margin-bottom: 20px;
                    }}
                    .email {{
                        background: #d4edda;
                        border: 1px solid #c3e6cb;
                        color: #155724;
                        padding: 15px;
                        border-radius: 8px;
                        margin: 20px 0;
                    }}
                    .btn {{
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 12px 30px;
                        border: none;
                        border-radius: 8px;
                        font-size: 16px;
                        cursor: pointer;
                        text-decoration: none;
                        display: inline-block;
                        margin: 10px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1 class="success">✅ Login Successful!</h1>
                    <div class="email">
                        Welcome, {session.get('user_email', 'User')}!
                    </div>
                    <p>You are now authenticated with AWS Cognito.</p>
                    <a href="/?authenticated=true" class="btn">Go to Dashboard</a>
                    <a href="/logout" class="btn">Sign Out</a>
                </div>
                <script>
                    // Store tokens in localStorage for React app
                    localStorage.setItem('cognitoIdToken', '{tokens.get('id_token', '')}');
                    localStorage.setItem('cognitoAccessToken', '{tokens.get('access_token', '')}');
                    localStorage.setItem('cognitoAuthenticated', 'true');
                </script>
            </body>
            </html>
            '''
        else:
            # Log detailed error for debugging
            error_info = f'''
            <html>
            <body style="font-family: monospace; padding: 20px;">
                <h2>Token Exchange Failed</h2>
                <p><strong>Status:</strong> {token_response.status_code}</p>
                <p><strong>Error:</strong> {token_response.text}</p>
                <p><strong>Auth Code (first 10 chars):</strong> {code[:10]}...</p>
                <hr>
                <p>This usually means:</p>
                <ul>
                    <li>The authorization code expired (codes are only valid for a few minutes)</li>
                    <li>The code was already used (codes can only be used once)</li>
                    <li>The redirect_uri doesn't match exactly what was configured</li>
                </ul>
                <p><a href="https://us-east-1k9zw8nznd.auth.us-east-1.amazoncognito.com/login?client_id=1da4nl8rlu0u3ec9n0hqke210c&response_type=code&scope=email+openid+phone&redirect_uri=https://d225ar6c86586s.cloudfront.net/callback">Try logging in again</a></p>
            </body>
            </html>
            '''
            return error_info, 400

    except Exception as e:
        return f'Callback error: {str(e)}', 500

@cognito_callback_bp.route('/logout')
def logout():
    """Logout from Cognito and clear session."""
    session.clear()

    # Redirect to Cognito logout
    logout_url = f'{USER_POOL_DOMAIN}/logout?client_id={CLIENT_ID}&logout_uri=https://d225ar6c86586s.cloudfront.net/'
    return redirect(logout_url)