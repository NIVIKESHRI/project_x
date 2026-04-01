from fyers_apiv3 import fyersModel
import webbrowser
from dotenv import load_dotenv
import os

load_dotenv()

client_id = os.getenv("FYERS_CLIENT_ID")
secret_key = os.getenv("FYERS_SECRET_KEY")
redirect_uri = os.getenv("FYERS_REDIRECT_URI")
access_token = os.getenv("FYERS_ACCESS_TOKEN")
# -----------------------------------------

# Create a session to generate the auth URL
session = fyersModel.SessionModel(
    client_id=client_id,
    secret_key=secret_key,
    redirect_uri=redirect_uri,
    response_type="code",
    state="sample_state"
)

# Generate the URL and open it in your browser
auth_url = session.generate_authcode()
print("Please log in and authorize the app at this URL:\n", auth_url)
webbrowser.open(auth_url)