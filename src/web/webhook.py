from flask import Flask, request, jsonify, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import hmac
import hashlib
import os
import logging
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load secrets from environment variables
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')
if not WEBHOOK_SECRET:
    raise ValueError("WEBHOOK_SECRET environment variable must be set")

# Initialize rate limiter (100 requests per hour per IP)
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["100 per hour"],
    storage_uri="memory://"
)

def verify_hmac_signature(payload: bytes, signature: str, secret: str) -> bool:
    """
    Verify HMAC signature from webhook request.
    
    Args:
        payload: Raw request body bytes
        signature: Signature from request header
        secret: Shared secret key
        
    Returns:
        True if signature is valid, False otherwise
    """
    if not signature:
        return False
    
    # Remove 'sha256=' prefix if present (GitHub/Jira style)
    if signature.startswith('sha256='):
        signature = signature[7:]
    
    expected_signature = hmac.new(
        secret.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected_signature, signature)

def validate_request():
    """
    Validate incoming webhook request for security.
    
    Checks:
    - Content-Type is application/json
    - Required headers are present
    - HMAC signature is valid (if provided)
    - Payload size is reasonable
    
    Raises:
        400/401/403 HTTP errors if validation fails
    """
    # Check Content-Type
    if request.content_type and 'application/json' not in request.content_type:
        logger.warning(f"Invalid Content-Type from {request.remote_addr}: {request.content_type}")
        abort(400, description="Content-Type must be application/json")
    
    # Check payload size (max 1MB)
    if request.content_length and request.content_length > 1024 * 1024:
        logger.warning(f"Payload too large from {request.remote_addr}: {request.content_length} bytes")
        abort(413, description="Payload too large")
    
    # Verify HMAC signature if provided
    signature = request.headers.get('X-Hub-Signature-256') or request.headers.get('X-Webhook-Signature')
    token = request.headers.get('X-Webhook-Token')
    
    if signature:
        # Get raw data for HMAC verification
        try:
            payload = request.get_data()
            if not verify_hmac_signature(payload, signature, WEBHOOK_SECRET):
                logger.error(f"Invalid HMAC signature from {request.remote_addr}")
                abort(401, description="Invalid signature")
            logger.info(f"Valid HMAC signature verified from {request.remote_addr}")
        except Exception as e:
            logger.error(f"Error verifying HMAC from {request.remote_addr}: {e}")
            abort(401, description="Invalid signature")
    elif token:
        # Check for simple token authentication as fallback
        if token != WEBHOOK_SECRET:
            logger.error(f"Invalid token from {request.remote_addr}")
            abort(401, description="Invalid authentication")
    else:
        logger.error(f"No authentication provided from {request.remote_addr}")
        abort(401, description="Missing authentication")

@app.before_request
def log_request():
    """Log all incoming requests for security monitoring."""
    logger.info(f"Request from {request.remote_addr}: {request.method} {request.path}")
    logger.info(f"Headers: {dict(request.headers)}")
    logger.info(f"Content-Type: {request.content_type}")
    logger.info(f"Content-Length: {request.content_length}")
    if request.method == 'POST':
        try:
            raw_data = request.get_data()
            logger.info(f"Raw data (bytes): {raw_data}")
            logger.info(f"Raw data (string): {raw_data.decode('utf-8', errors='replace')}")
        except Exception as e:
            logger.error(f"Error reading raw data: {e}")

@app.route('/webhook', methods=['POST'])
@limiter.limit("100 per hour")
def webhook_receiver():
    """
    Secure webhook endpoint with authentication and rate limiting.
    
    Expected headers:
    - Content-Type: application/json
    - X-Hub-Signature-256: sha256=<hmac_signature> (recommended)
    OR
    - X-Webhook-Token: <secret_token> (fallback)
    
    Returns:
        JSON response with status
    """
    try:
        # Validate request security
        validate_request()
        
        # Get and validate JSON data
        try:
            data = request.get_json(force=True)
        except Exception as json_error:
            logger.warning(f"JSON parsing error from {request.remote_addr}: {json_error}")
            abort(400, description="Invalid JSON payload")
        
        if not data:
            logger.warning(f"Empty payload from {request.remote_addr}")
            abort(400, description="Empty payload")
        
        # Log successful webhook receipt
        logger.info(f"Valid webhook received from {request.remote_addr}")
        logger.debug(f"Webhook data: {data}")
        
        # Forward webhook data to TM application
        try:
            tm_response = requests.post(
                'http://localhost:5001/webhook',
                json=data,
                timeout=5
            )
            
            if tm_response.status_code == 200:
                logger.info("Successfully forwarded webhook to TM application")
            else:
                logger.warning(f"TM application returned status {tm_response.status_code}: {tm_response.text}")
                
        except requests.exceptions.ConnectionError:
            logger.error("Failed to connect to TM application at localhost:5001 - is it running?")
        except requests.exceptions.Timeout:
            logger.error("Timeout connecting to TM application")
        except Exception as e:
            logger.error(f"Error forwarding to TM application: {e}")
        
        return jsonify({
            'status': 'success',
            'message': 'Webhook received and processed successfully',
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing webhook from {request.remote_addr}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint (no authentication required)."""
    return jsonify({'status': 'healthy'}), 200

@app.errorhandler(429)
def ratelimit_handler(e):
    """Handle rate limit exceeded."""
    logger.warning(f"Rate limit exceeded for {request.remote_addr}")
    return jsonify({
        'status': 'error',
        'message': 'Rate limit exceeded'
    }), 429

if __name__ == '__main__':
    # Never run with debug=True in production
    # Use a production WSGI server like gunicorn instead
    logger.info("Starting webhook server...")
    logger.warning("Running in development mode - use gunicorn for production")
    app.run(host='127.0.0.1', port=8000, debug=False)