import json
import time
import logging
import threading
import websocket
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class EaseApiTicker:
    """
    EaseAPI WebSocket client for subscribing to real-time market data and order status.
    """
    
    # Base WebSocket URL
    BASE_WS_URL = "wss://easeapi-ws.venturasecurities.com"
    
    # WebSocket endpoint paths
    MARKET_DATA_PATH = "/v1/easeapi_mktdata"
    ORDER_STATUS_PATH = "/v1/easeapi_ob"
    
    # Constants for supported exchanges
    EXCHANGE_NSE = "nse"
    EXCHANGE_BSE = "bse"
    EXCHANGE_FNO = "fno"  # NSE Futures & Options
    EXCHANGE_BFO = "bfo"  # BSE Futures & Options
    
    def __init__(self, app_key, client_id, auth_token):
        """
        Initialize the EaseApiTicker.
        
        Parameters:
        -----------
        app_key : str
            Your EaseAPI application key
        client_id : str
            Your client ID
        auth_token : str
            Your authentication token
        """
        self.app_key = app_key
        self.client_id = client_id
        self.auth_token = auth_token
        self.ws = None
        self.ws_thread = None
        
        # URL for market data
        market_query = urlencode(
            {"app_key": app_key, "client_id": client_id, "authorization": auth_token}
        )
        self.market_data_url = f"{self.BASE_WS_URL}{self.MARKET_DATA_PATH}?{market_query}"
        
        # URL for order status
        order_query = urlencode(
            {"app_key": app_key, "client_id": client_id, "authorization": auth_token}
        )
        self.order_status_url = f"{self.BASE_WS_URL}{self.ORDER_STATUS_PATH}?{order_query}"
        
        # By default, we'll connect to market data
        self.ws_url = self.market_data_url
        
        # Connection state
        self.connected = False
        self.connecting = False
        
        # Reconnection settings
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_interval = 1 
        self.reconnect_timer = None
        
        # Subscribed instruments - store as dict with exchange as key and set of tokens as value. e.g. {"nse": {"2885", "15"}, "bse": {"500570"}}
        self.subscriptions = {}
        
        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self.on_error = None
        self.on_reconnect = None
        self.on_noreconnect = None
    
    def connect(self, use_order_status=False):
        """
        Establish connection to the WebSocket server.
        
        Parameters:
        -----------
        use_order_status : bool
            If True, connect to order status endpoint.
            If False (default), connect to market data endpoint.
        """
        if self.connecting or self.connected:
            logger.debug("Already connected or connecting to WebSocket")
            return
        
        # Select the appropriate WebSocket URL
        self.ws_url = self.order_status_url if use_order_status else self.market_data_url
        endpoint_type = "order status" if use_order_status else "market data"
            
        self.connecting = True
        logger.info(f"Connecting to EaseAPI WebSocket ({endpoint_type})...")
        
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={'ping_interval': 30})
        self.ws_thread.daemon = True
        self.ws_thread.start()
    
    def _on_open(self, ws):
        """WebSocket on_open event handler."""
        logger.info("WebSocket connected successfully")
        self.connecting = False
        self.connected = True
        self.reconnect_attempts = 0
        
        # Resubscribe to previously subscribed instruments
        self._resubscribe()
        
        # Call user-defined callback
        if self.on_connect:
            self.on_connect(self, {})
    
    def _on_message(self, ws, message):
        """WebSocket on_message event handler."""
        try:
            data = json.loads(message)
            if self.on_ticks:
                self.on_ticks(self, data)
            else:
                logger.debug(f"Received tick data: {data}")
        except json.JSONDecodeError:
            logger.warning(f"Received non-JSON message: {message}")
    
    def _on_error(self, ws, error):
        """WebSocket on_error event handler."""
        logger.error(f"WebSocket error: {error}")
        self.connecting = False
        
        if self.on_error:
            self.on_error(self, None, error)
    
    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket on_close event handler."""
        logger.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connecting = False
        self.connected = False
        
        if self.on_close:
            self.on_close(self, close_status_code, close_msg)
        
        # Always attempt to reconnect
        self._handle_reconnect()
    
    def _handle_reconnect(self):
        """Handle reconnection with fixed 1-second interval."""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached")
            if self.on_noreconnect:
                self.on_noreconnect(self)
            return
            
        self.reconnect_attempts += 1
        
        logger.info(f"Attempting to reconnect ({self.reconnect_attempts}/{self.max_reconnect_attempts}) in {self.reconnect_interval}s...")
        
        # Notify about reconnection attempt
        if self.on_reconnect:
            self.on_reconnect(self, self.reconnect_attempts)
        
        # Cancel existing timer if any
        if self.reconnect_timer:
            self.reconnect_timer.cancel()
            
        # We'll reconnect to the same endpoint that was previously connected
        self.reconnect_timer = threading.Timer(self.reconnect_interval, self._reconnect)
        self.reconnect_timer.daemon = True
        self.reconnect_timer.start()
    
    def _reconnect(self):
        """Reconnect to the same endpoint that was previously connected."""
        use_order_status = (self.ws_url == self.order_status_url)
        self.connect(use_order_status=use_order_status)
    
    def _resubscribe(self):
        """Resubscribe to all previously subscribed instruments after reconnection."""
        if not self.subscriptions:
            return
            
        logger.info(f"Resubscribing to {len(self.subscriptions)} exchanges")
        
        for exchange, tokens in self.subscriptions.items():
            if tokens:
                self.subscribe(list(tokens), exchange, skip_add=True)
    
    def _get_action_key(self, exchange):
        """Generate a key for the LTP action."""
        if exchange not in [self.EXCHANGE_NSE, self.EXCHANGE_BSE, self.EXCHANGE_FNO, self.EXCHANGE_BFO]:
            logger.warning(f"Unsupported exchange: {exchange}. Using NSE as default.")
            exchange = self.EXCHANGE_NSE
            
        return f"{exchange}:ltp"
    
    def subscribe(self, tokens, exchange="nse", skip_add=False):
        """
        Subscribe to LTP market data for specified tokens.
        
        Parameters:
        -----------
        tokens : str, int, or list
            Single token or list of tokens to subscribe
        exchange : str, optional
            Exchange to subscribe to (default: "nse")
            Supported exchanges: "nse", "bse", "fno", "bfo"
        skip_add : bool, optional
            If True, don't update subscriptions list (used for resubscribing)
        
        Returns:
        --------
        bool
            True if subscription request was sent, False otherwise
        """
        if not self.connected:
            logger.warning("Cannot subscribe, WebSocket not connected")
            return False
        
        # Validate exchange
        if exchange not in [self.EXCHANGE_NSE, self.EXCHANGE_BSE, self.EXCHANGE_FNO, self.EXCHANGE_BFO]:
            logger.warning(f"Unsupported exchange: {exchange}. Using NSE as default.")
            exchange = self.EXCHANGE_NSE
        
        # Convert single token to list
        if not isinstance(tokens, list):
            tokens = [str(tokens)]
        else:
            # Ensure all tokens are strings
            tokens = [str(token) for token in tokens]
            
        # Prepare subscription message
        action_key = self._get_action_key(exchange)
        actions = [action_key]
        
        # Update subscription dictionary
        if not skip_add:
            if exchange not in self.subscriptions:
                self.subscriptions[exchange] = set()
            
            # Add tokens to the subscription set
            self.subscriptions[exchange].update(tokens)
        
        # Create subscription message
        subscribe_msg = {
            "actions": actions,
            "token": tokens,
            "mode": "sub"
        }
        
        return self.send(subscribe_msg)
    
    def unsubscribe(self, tokens, exchange="nse"):
        """
        Unsubscribe from LTP market data for specified tokens.
        
        Parameters:
        -----------
        tokens : str, int, or list
            Single token or list of tokens to unsubscribe
        exchange : str, optional
            Exchange to unsubscribe from (default: "nse")
            Supported exchanges: "nse", "bse", "fno", "bfo"
            
        Returns:
        --------
        bool
            True if unsubscription request was sent, False otherwise
        """
        if not self.connected:
            logger.warning("Cannot unsubscribe, WebSocket not connected")
            return False
        
        # Validate exchange
        if exchange not in [self.EXCHANGE_NSE, self.EXCHANGE_BSE, self.EXCHANGE_FNO, self.EXCHANGE_BFO]:
            logger.warning(f"Unsupported exchange: {exchange}. Using NSE as default.")
            exchange = self.EXCHANGE_NSE
        
        # Convert single token to list
        if not isinstance(tokens, list):
            tokens = [str(tokens)]
        else:
            # Ensure all tokens are strings
            tokens = [str(token) for token in tokens]
        
        # Prepare unsubscription message
        action_key = self._get_action_key(exchange)
        actions = [action_key]
        
        # Update subscription dictionary
        if exchange in self.subscriptions:
            # Remove tokens from the subscription set
            self.subscriptions[exchange].difference_update(tokens)
            
            # Remove the exchange key if no tokens left
            if not self.subscriptions[exchange]:
                del self.subscriptions[exchange]
        
        # Create unsubscription message
        unsubscribe_msg = {
            "actions": actions,
            "token": tokens,
            "mode": "unsub"
        }
        
        return self.send(unsubscribe_msg)
    
    def send(self, data):
        """
        Send data to the WebSocket server.
        
        Parameters:
        -----------
        data : dict or str
            Data to send to the server
            
        Returns:
        --------
        bool
            True if the message was sent, False otherwise
        """
        if not self.connected:
            logger.warning("Cannot send message, WebSocket not connected")
            return False
        
        try:
            if isinstance(data, dict):
                data = json.dumps(data)
            self.ws.send(data)
            return True
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            return False
    
    def close(self):
        """Close the WebSocket connection."""
        # Cancel any pending reconnection
        if self.reconnect_timer:
            self.reconnect_timer.cancel()
        
        if self.ws:
            self.ws.close()
            
        self.connected = False
        logger.info("WebSocket connection closed")
        
    def is_connected(self):
        """
        Check if WebSocket connection is established.
        
        Returns:
        --------
        bool
            True if connected, False otherwise
        """
        return self.connected