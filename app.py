from flask import Flask, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
import json
import redis
import logging

# --- Flask App Initialization ---
app = Flask(__name__) # If your file is main.py, 'app' is a common variable name
CORS(app) # Apply CORS to all routes, including SocketIO

# --- Configuration ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///rides.db' # Database file
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = '123456' # IMPORTANT: Change this!

# --- Redis Configuration (Hardcoded as requested) ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
redis_client = None # Initialize to None

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s [%(filename)s:%(lineno)d]')

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()
    logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT} - {e}. Real-time features might be impaired.")
    # App can continue but SocketIO might not scale well / some features might fail.

# --- Database ---
db = SQLAlchemy(app)

# --- SocketIO ---
if redis_client:
    socketio = SocketIO(app, cors_allowed_origins="*", message_queue=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
    logging.info("SocketIO initialized with Redis message queue.")
else:
    logging.warning("SocketIO initialized WITHOUT Redis message queue. Scalability will be limited.")
    socketio = SocketIO(app, cors_allowed_origins="*")


# --- Database Model ---
class Rider(db.Model):
    __tablename__ = 'riders'
    userName = db.Column(db.String(80), primary_key=True)
    ride_code = db.Column(db.String(20), nullable=True, index=True) # Added index for faster lookups
    source = db.Column(db.String(250),nullable=True) # JSON: {latitude, longitude, address}
    destination = db.Column(db.String(250),nullable=True) # JSON: {latitude, longitude, address} - Set by owner
    stops = db.Column(db.String(500),nullable=True) # JSON: array of stop objects - Set by owner
    # distance_travelled = db.Column(db.Float, default=0.0,nullable=True) # Not used in real-time part yet
    # average_speed = db.Column(db.Float, default=0.0,nullable=True) # Not used
    owner=db.Column(db.String(80), nullable=True) # userName of the ride creator/owner
    status=db.Column(db.String(20),nullable=True) # e.g., "created", "joined", "active"

# Create database tables if they don't exist
with app.app_context():
    db.create_all()
    logging.info("Database tables checked/created.")

# --- Helper for Redis Keys ---
def get_ride_locations_key(ride_code):
    return f"ride_locations:{ride_code}" # Stores hash: {userName: "lat,lng"}

# --- HTTP Endpoints ---
@app.route('/api/riders', methods=['POST'])
def create_rider_endpoint():
    data = request.get_json()
    if not data or 'userName' not in data:
        logging.warning("Attempt to create rider without userName.")
        return jsonify({'error': 'Missing required field: userName'}), 400

    user_name = data['userName']
    existing_rider = Rider.query.filter_by(userName=user_name).first()
    if existing_rider:
        logging.info(f"Rider '{user_name}' already exists. Returning existing info.")
        return jsonify({'userName': existing_rider.userName, 'message': 'Rider already exists'}), 200

    try:
        rider = Rider(userName=user_name)
        db.session.add(rider)
        db.session.commit()
        logging.info(f"Created new rider: '{user_name}'")
        return jsonify({'userName': rider.userName}), 201
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error creating rider '{user_name}': {e}", exc_info=True)
        return jsonify({'error': 'Failed to create rider. ' + str(e)}), 500


@app.route('/api/info', methods=['POST'])
def ride_info():
    data = request.get_json()
    userName = data.get('userName')
    pickup_data = data.get('pickup') # Expected: {latitude, longitude, address}
    destination_data = data.get('destination')
    stops_data = data.get('stops', [])
    ride_code = data.get('generatedCode')
    owner_userName = data.get('owner')
    status = data.get('status')

    if not all([userName, ride_code, owner_userName]):
        missing_fields = [f for f, v in [("userName", userName), ("generatedCode", ride_code), ("owner", owner_userName)] if not v]
        logging.warning(f"/api/info validation failed. Missing: {', '.join(missing_fields)}")
        return jsonify({'success': False, 'message': f"Missing required fields: {', '.join(missing_fields)}"}), 400

    rider = Rider.query.filter_by(userName=userName).first()
    if not rider:
        logging.info(f"Rider '{userName}' not found in /api/info, creating.")
        rider = Rider(userName=userName)
        db.session.add(rider)
    
    rider.ride_code = ride_code
    # Only update source if new pickup_data is provided, otherwise keep existing
    if pickup_data and 'latitude' in pickup_data and 'longitude' in pickup_data:
        rider.source = json.dumps(pickup_data)
    rider.owner = owner_userName # This establishes who the ride creator is for this participant
    rider.status = status if status else rider.status

    # Only the owner of the ride can set/update the destination and stops for that ride_code
    # We ensure this by checking if the current userName is the owner_userName they are claiming.
    if userName == owner_userName:
        owner_record_to_update = Rider.query.filter_by(userName=owner_userName, ride_code=ride_code).first()
        if not owner_record_to_update: # Should be the same as 'rider' if userName == owner_userName
            owner_record_to_update = rider # Assign current rider as owner_record if creating
        
        if destination_data:
             owner_record_to_update.destination = json.dumps(destination_data)
        if stops_data: # Check if stops_data is not None and not empty list if that's intended
             owner_record_to_update.stops = json.dumps(stops_data)
        # If 'rider' is different from 'owner_record_to_update' because we looked up an existing owner,
        # ensure the owner's ride_code is also set if they are creating the ride here.
        if owner_record_to_update != rider:
            owner_record_to_update.ride_code = ride_code # Ensure owner has ride_code if it's a new ride def
            db.session.add(owner_record_to_update) # If it was a different record

    try:
        db.session.commit()
        logging.info(f"Ride info processed for '{userName}' in ride '{ride_code}'. Owner: '{owner_userName}'.")
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error saving ride info for '{userName}': {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'Database error saving ride info. ' + str(e)}), 500

    # Store/Update initial location in Redis if pickup_data is valid
    if redis_client and rider.source: # Use rider.source which is now committed
        try:
            current_pickup = json.loads(rider.source)
            if 'latitude' in current_pickup and 'longitude' in current_pickup:
                locations_key = get_ride_locations_key(ride_code)
                redis_client.hset(locations_key, userName, f"{current_pickup['latitude']},{current_pickup['longitude']}")
                logging.info(f"Set initial Redis location for '{userName}' in '{ride_code}'.")
                # redis_client.expire(locations_key, 3600 * 12) # Expire ride data after 12 hours
        except (TypeError, json.JSONDecodeError) as e:
            logging.error(f"Error processing rider.source for Redis update: {e}", exc_info=True)


    # Fetch the definitive destination/stops from the owner's record for the response
    final_owner_record = Rider.query.filter_by(userName=owner_userName, ride_code=ride_code).first()
    final_destination = json.loads(final_owner_record.destination) if final_owner_record and final_owner_record.destination else None
    final_stops = json.loads(final_owner_record.stops) if final_owner_record and final_owner_record.stops else []
    
    return jsonify({
        'success': True, 
        'message': 'Ride info processed successfully.', 
        'rideData': {
            'userName': rider.userName,
            'pickup': json.loads(rider.source) if rider.source else None,
            'destination': final_destination,
            'stops': final_stops,
            'rideCode': rider.ride_code,
            'ownerUserName': rider.owner,
            'status': rider.status
        }
    }), 200


@app.route('/api/rider_current_ride/<string:userName>', methods=['GET'])
def get_rider_current_ride_info(userName):
    logging.info(f"API call: /api/rider_current_ride/{userName}")
    rider = Rider.query.filter_by(userName=userName).first()

    if not rider:
        logging.warning(f"Rider '{userName}' not found for /rider_current_ride.")
        return jsonify({'error': 'Rider not found.'}), 404

    if not rider.ride_code:
        logging.warning(f"Rider '{userName}' found, but no active ride_code associated.")
        return jsonify({'error': 'No active ride found for this user. Please create or join a ride.'}), 404
    
    ride_code = rider.ride_code
    owner_userName = rider.owner 

    if not owner_userName:
        logging.error(f"Data integrity issue: Rider '{userName}' in ride '{ride_code}' has no owner assigned.")
        return jsonify({'error': 'Ride data is incomplete (missing owner). Please rejoin the ride.'}), 500

    # Fetch the definitive ride details (destination, stops) from the owner's record
    owner_record = Rider.query.filter_by(userName=owner_userName, ride_code=ride_code).first()
    if not owner_record:
        logging.error(f"Consistency error: Owner '{owner_userName}' record not found for ride '{ride_code}' (user: '{userName}').")
        return jsonify({'error': 'Ride owner details are inconsistent or missing. The ride might have ended or data is corrupt.'}), 500

    ride_destination = json.loads(owner_record.destination) if owner_record.destination else None
    # ride_stops = json.loads(owner_record.stops) if owner_record.stops else [] # Include if needed

    user_pickup = json.loads(rider.source) if rider.source else None

    co_riders_locations = []
    if redis_client:
        locations_key = get_ride_locations_key(ride_code)
        all_redis_locations = redis_client.hgetall(locations_key)
        for r_name, loc_str in all_redis_locations.items():
            if r_name != userName:
                try:
                    lat, lng = map(float, loc_str.split(','))
                    co_riders_locations.append({'userName': r_name, 'latitude': lat, 'longitude': lng})
                except (ValueError, TypeError) as e:
                    logging.warning(f"Redis parse error for '{r_name}' in '{ride_code}' during get_rider_current_ride_info: '{loc_str}', Error: {e}")
    
    logging.info(f"Successfully fetched current ride info for '{userName}': rideCode='{ride_code}', owner='{owner_userName}'.")
    return jsonify({
        'userName': userName,
        'rideCode': ride_code,
        'ownerUserName': owner_userName,
        'pickup': user_pickup,
        'destination': ride_destination,
        # 'stops': ride_stops,
        'initialCoRiders': co_riders_locations
    }), 200


@app.route('/api/ride/<ride_code>', methods=['GET'])
def get_ride_by_code_general(ride_code):
    logging.info(f"API call: /api/ride/{ride_code} (general info)")
    any_participant = Rider.query.filter_by(ride_code=ride_code).first()
    if not any_participant:
        logging.warning(f"No participant found for ride_code '{ride_code}' in general info request.")
        return jsonify({'error': f'Ride with code {ride_code} not found or has no participants.'}), 404
    
    owner_userName = any_participant.owner
    if not owner_userName:
        logging.error(f"Data integrity issue: Ride '{ride_code}' has participant '{any_participant.userName}' with no owner.")
        return jsonify({'error': 'Ride data is incomplete (missing owner).'}), 500
        
    owner_record = Rider.query.filter_by(userName=owner_userName, ride_code=ride_code).first()
    if not owner_record:
        logging.error(f"Consistency error: Owner '{owner_userName}' not found for ride '{ride_code}' (general info).")
        return jsonify({'error': 'Ride owner information inconsistent or not found.'}), 500

    destination_info = json.loads(owner_record.destination) if owner_record.destination else None
    stops_info = json.loads(owner_record.stops) if owner_record.stops else []
    
    db_participants = Rider.query.filter_by(ride_code=ride_code).all()
    participants_details = []
    current_locations_redis = {}
    if redis_client:
        redis_locations_key = get_ride_locations_key(ride_code)
        current_locations_redis = redis_client.hgetall(redis_locations_key)

    for p_db in db_participants:
        user_name = p_db.userName
        current_loc_str = current_locations_redis.get(user_name)
        current_lat, current_lng = None, None
        if current_loc_str:
            try:
                current_lat, current_lng = map(float, current_loc_str.split(','))
            except (ValueError, TypeError) as e:
                logging.warning(f"Redis parse error for '{user_name}' in '{ride_code}' (general info): '{current_loc_str}', Error: {e}")
        
        participants_details.append({
            "userName": user_name,
            "source": json.loads(p_db.source) if p_db.source else None,
            "currentLocation": {"latitude": current_lat, "longitude": current_lng} if current_lat is not None and current_lng is not None else None,
            "isOwner": user_name == owner_userName
        })

    return jsonify({
        'rideCode': ride_code,
        'destination': destination_info,
        'stops': stops_info,
        'participants': participants_details,
        'ownerUserName': owner_userName
    }), 200


# --- SocketIO Event Handlers ---
@socketio.on('connect')
def handle_connect():
    logging.info(f"Client connected: sid={request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    logging.info(f"Client disconnected: sid={request.sid}")
    # TODO: Implement a robust way to find associated userName and rideCode for this sid
    # and emit 'user_left_ride' if they were in a room. This often involves storing
    # a mapping like {sid: {userName, rideCode}} when they join a room.
    # For now, relies on explicit 'leave_ride_room' from client.

@socketio.on('join_ride_room')
def on_join_ride_room(data):
    userName = data.get('userName')
    rideCode = data.get('rideCode')
    sid = request.sid
    logging.info(f"Socket 'join_ride_room': userName='{userName}', rideCode='{rideCode}', sid={sid}")

    if not userName or not rideCode:
        emit('error_event', {'message': 'userName and rideCode are required for join_ride_room.'}, room=sid)
        logging.warning(f"Join attempt failed for sid {sid}: Missing userName or rideCode.")
        return

    # Verify user is part of this ride in DB (optional, but good for security/consistency)
    # rider_check = Rider.query.filter_by(userName=userName, ride_code=rideCode).first()
    # if not rider_check:
    #     emit('error_event', {'message': 'You are not registered for this ride.'}, room=sid)
    #     logging.warning(f"User '{userName}' (sid {sid}) attempted to join room '{rideCode}' but not in DB for this ride.")
    #     return

    join_room(rideCode, sid=sid) # Explicitly pass sid for clarity
    logging.info(f"User '{userName}' (sid: {sid}) successfully joined Socket.IO room '{rideCode}'")

    # Store sid to user mapping if needed for disconnect cleanup (example)
    # if not hasattr(g, 'user_sids'): g.user_sids = {}
    # g.user_sids[sid] = {'userName': userName, 'rideCode': rideCode}


    other_riders_locations = []
    if redis_client:
        locations_key = get_ride_locations_key(rideCode)
        all_redis_locations = redis_client.hgetall(locations_key)
        for r_name, loc_str in all_redis_locations.items():
            if r_name != userName:
                try:
                    lat, lng = map(float, loc_str.split(','))
                    other_riders_locations.append({'userName': r_name, 'latitude': lat, 'longitude': lng})
                except (ValueError, TypeError) as e:
                    logging.warning(f"Redis parse error for '{r_name}' in '{rideCode}' during join notification: '{loc_str}', Error: {e}")
    
    if other_riders_locations:
        emit('initial_co_riders', {'coRiders': other_riders_locations}, room=sid)
        logging.info(f"Sent 'initial_co_riders' ({len(other_riders_locations)} riders) to '{userName}' (sid {sid}) for ride '{rideCode}'.")

    joining_user_location = None
    if redis_client:
        joining_user_loc_str = redis_client.hget(get_ride_locations_key(rideCode), userName)
        if joining_user_loc_str:
            try:
                lat, lng = map(float, joining_user_loc_str.split(','))
                joining_user_location = {'latitude': lat, 'longitude': lng}
            except (ValueError, TypeError) as e:
                logging.warning(f"Redis parse error for joining user '{userName}' location: '{joining_user_loc_str}', Error: {e}")

    if joining_user_location:
        socketio.emit('user_joined_ride', {
            'userName': userName,
            'latitude': joining_user_location['latitude'],
            'longitude': joining_user_location['longitude']
        }, to=rideCode, include_self=False)
        logging.info(f"Notified room '{rideCode}' that '{userName}' joined with location.")
    else:
        socketio.emit('user_joined_ride', {'userName': userName}, to=rideCode, include_self=False)
        logging.info(f"Notified room '{rideCode}' that '{userName}' joined (location pending client update).")

    emit('join_success', {'message': f'Successfully joined room {rideCode}'}, room=sid)


@socketio.on('update_location')
def handle_location_update(data):
    userName = data.get('userName')
    rideCode = data.get('rideCode')
    latitude = data.get('latitude')
    longitude = data.get('longitude')
    sid = request.sid
    # logging.debug(f"Socket 'update_location': {userName}, {rideCode}, lat={latitude}, lng={longitude}, sid={sid}") # Noisy

    if not all([userName, rideCode, latitude is not None, longitude is not None]):
        logging.warning(f"Invalid location update from '{userName}' (sid {sid}): Missing data - {data}")
        # emit('error_event', {'message': 'Invalid location data for update.'}, room=sid) # Optional
        return
    
    if redis_client:
        locations_key = get_ride_locations_key(rideCode)
        redis_client.hset(locations_key, userName, f"{latitude},{longitude}")

        socketio.emit('location_update_from_server', {
            'userName': userName,
            'latitude': latitude,
            'longitude': longitude
        }, to=rideCode, include_self=False)
    else:
        logging.error("Redis not available. Cannot process 'update_location'.")
        emit('error_event', {'message': 'Server error: Location service temporarily unavailable.'}, room=sid)


@socketio.on('leave_ride_room')
def on_leave_ride_room(data):
    userName = data.get('userName')
    rideCode = data.get('rideCode')
    sid = request.sid # sid of the client emitting 'leave_ride_room'
    logging.info(f"Socket 'leave_ride_room': userName='{userName}', rideCode='{rideCode}', sid={sid}")

    if not userName or not rideCode:
        logging.warning(f"Invalid 'leave_ride_room' data from sid {sid}: {data}")
        return

    # It's good practice to ensure the client calling leave_room is actually in that room
    # However, leave_room(room_name, sid=sid) will safely do nothing if sid isn't in room_name.
    leave_room(rideCode, sid=sid)
    logging.info(f"User '{userName}' (sid: {sid}) explicitly left Socket.IO room '{rideCode}'")

    # Remove from sid to user mapping if used for disconnect cleanup
    # if hasattr(g, 'user_sids') and sid in g.user_sids: del g.user_sids[sid]

    if redis_client:
        locations_key = get_ride_locations_key(rideCode)
        redis_client.hdel(locations_key, userName)
        logging.info(f"Removed '{userName}' from Redis locations for ride '{rideCode}'.")

        if redis_client.hlen(locations_key) == 0:
            redis_client.delete(locations_key)
            logging.info(f"Ride '{rideCode}' locations key deleted from Redis as it's empty.")

    socketio.emit('user_left_ride', {'userName': userName, 'rideCode': rideCode}, to=rideCode, include_self=False)
    logging.info(f"Notified room '{rideCode}' that '{userName}' left.")

    # Optional: Update user status in DB
    # rider = Rider.query.filter_by(userName=userName, ride_code=rideCode).first()
    # if rider:
    #     rider.status = "left_ride" 
    #     # rider.ride_code = None # Or clear ride_code if they can't rejoin this specific instance
    #     db.session.commit()

# --- Main Execution ---
if __name__ == '__main__':
    logging.info("Attempting to start Flask-SocketIO server...")
    # For production, use a proper WSGI server like Gunicorn with eventlet or gevent workers:
    # gunicorn --worker-class eventlet -w 1 main:app
    # Ensure 'eventlet' is installed: pip install eventlet
    socketio.run(app, debug=True, host="0.0.0.0", port=5000, use_reloader=True, allow_unsafe_werkzeug=True if app.debug else False)
    # `allow_unsafe_werkzeug=True` might be needed with `use_reloader=True` and newer Werkzeug versions for debug mode.
    # For more stability during development, consider `use_reloader=False` if you face issues.