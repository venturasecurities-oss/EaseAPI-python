import time
import logging
from easeapi import EaseApiTicker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared credentials - update these in one place
APP_KEY = "VUz2rue5R0mYqe3jx5Ic"
CLIENT_ID = "AA0605"
AUTH_TOKEN = "eyJraWQiOiIrSzhkR1pWRzhBRCt1bnVLRlFTS1NFOGN4TE1DMXJMaThFMzFSeWRKbkxJPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI4ZmUzYjgwOS1lMjcyLTQ3ODItOTE2Yi0zNDhhZmViZGU4OWUiLCJpc3MiOiJodHRwczovL2NvZ25pdG8taWRwLmFwLXNvdXRoLTEuYW1hem9uYXdzLmNvbS9hcC1zb3V0aC0xX0N3aVo5a0dzdSIsImNsaWVudF9pZCI6IjMwZWU4aW85dGljOTc1b3ZpODdpMnBncGR2Iiwib3JpZ2luX2p0aSI6IjcyZDZjODUxLWIyNTItNDFiMy04YTg0LWExMjg1ZWM5NGJiMSIsImV2ZW50X2lkIjoiZGMwN2YwODQtNDRhMi00NWUwLTljNjQtZWEwYTNmYzc1NWVhIiwidG9rZW5fdXNlIjoiYWNjZXNzIiwic2NvcGUiOiJhd3MuY29nbml0by5zaWduaW4udXNlci5hZG1pbiIsImF1dGhfdGltZSI6MTc5MTc3NzY0NCwiZXhwIjoxNzkxODY0MDQ0LCJpYXQiOjE3OTE3Nzc2NDQsImp0aSI6IjliN2ViMGE3LWMyN2MtNDEwNi1iZTM5LTU1ZTE4MDc5OGRmNyIsInVzZXJuYW1lIjoiYWEwNjA1In0.fTP9Aw0n9Gw8qmk3CKFHzUKrky7qhAD6gzc-CROttx2q954gvIGSL-qUOFd8ogOfHEHy23FEiqGNOjyF4vO8xdCvxde-85m05fZxHWNsM2fUQu9RZ6wWyBKNU4NK2Op0Xnj4o9H4XgzMxluPtXGgQfH92QPLF2q6cOeb8R6CIkPmdBG0djcvm9XvHr9_sz-qxhEKLvgL5VMAgIW5z8LlbDvdazxwskg5GEP5ObppQTsP2mdQyjuy64Vs2gzka7X2fX0mE8y8D6_DLi2HbAfxFoBt1P35qmAhG9FYrLgH9--NdWTGgW_qtNf7RO2zMMzIEAOtH0MBdRCcM6EVNXhDpw"

ticker = EaseApiTicker(
    app_key=APP_KEY,
    client_id=CLIENT_ID,
    auth_token=AUTH_TOKEN,
)

def on_ticks(ws, tick_data):
    logging.info(f"Tick: {tick_data}")

def on_connect(ws, response):
    logging.info("Connected!")
    
    # For market data connection
    if ws.ws_url == ws.market_data_url:

        # Subscribe to instruments
        ws.subscribe(["2885", "11536"], exchange=ws.EXCHANGE_NSE)  # RELIANCE, TCS on NSE
        ws.subscribe(["500570"], exchange=ws.EXCHANGE_BSE)  # TATAMOTORS on BSE
        ws.subscribe(["Nifty 50"], exchange=ws.EXCHANGE_INDEX)  # NIFTY 50 index
    
    # For order status connection
    elif ws.ws_url == ws.order_status_url:
        logging.info("Connected to order status WebSocket")
        # order status websocket it will push updates automatically

def on_close(ws, code, reason):
    logging.info(f"Disconnected: {reason}")

# Set callbacks for market data
ticker.on_ticks = on_ticks
ticker.on_connect = on_connect
ticker.on_close = on_close

# Example 1: Connect to market data endpoint (default)
ticker.connect()

# Example 2: If you want to connect to order status endpoint instead
# Create a second instance for order status
order_ticker = EaseApiTicker(
    app_key=APP_KEY,
    client_id=CLIENT_ID,
    auth_token=AUTH_TOKEN,
)

# Set callbacks for order ticker
order_ticker.on_ticks = on_ticks
order_ticker.on_connect = on_connect
order_ticker.on_close = on_close

# Connect to order status endpoint
order_ticker.connect(use_order_status=True)

# Keep the program running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    ticker.close()
    order_ticker.close()
