from flask import Flask, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import json
import requests

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///rides.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
CORS(app)

class Rider(db.Model):
    __tablename__ = 'riders'
    userName = db.Column(db.String(80), primary_key=True)
    ride_code = db.Column(db.String(20), nullable=True)
    source = db.Column(db.String(250), nullable=True) # JSON string for source location details
    destination = db.Column(db.String(250), nullable=True) # JSON string for destination location details
    stops = db.Column(db.String(500), nullable=True) # JSON string for stops list
    distance_travelled = db.Column(db.Float, default=0.0, nullable=True)
    average_speed = db.Column(db.Float, default=0.0, nullable=True)
    owner = db.Column(db.String(250))
    status = db.Column(db.String(20), nullable=True)
    current_latitude = db.Column(db.Float, nullable=True)
    current_longitude = db.Column(db.Float, nullable=True)
    expo_push_token = db.Column(db.String(255), nullable=True)

    # REMOVED: pickup_latitude = db.Column(db.Float, nullable=True)
    # REMOVED: pickup_longitude = db.Column(db.Float, nullable=True)

with app.app_context():
    db.create_all()

@app.route('/api/riders', methods=['POST'])
def create_rider():
    data = request.get_json()
    if not data or 'userName' not in data:
        return jsonify({'error': 'Missing required field: userName'}), 400

    if db.session.get(Rider, data['userName']):
        return jsonify({'error': 'Rider with this name already exists'}), 409

    try:
        rider = Rider(userName=data['userName'])
        db.session.add(rider)
        db.session.commit()
        return jsonify({'userName': rider.userName}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/info', methods=['POST'])
def ride_info():
    data = request.get_json()
    userName = data.get('userName')
    pickup = data.get('pickup')
    destination = data.get('destination')
    stops = data.get('stops', [])
    ride_code = data.get('generatedCode')
    owner = data.get('owner')
    status = data.get('status')

    if not userName:
        return jsonify({'success': False, 'message': 'userName is required'}), 400

    rider = db.session.get(Rider, userName)
    if not rider:
        return jsonify({'success': False, 'message': 'Rider not found'}), 404

    rider.source = json.dumps(pickup) if pickup else None
    rider.destination = json.dumps(destination) if destination else None
    rider.current_latitude= pickup.get('latitude') if pickup and 'latitude' in pickup else None
    rider.current_longitude = pickup.get('longitude') if pickup and 'longitude' in pickup else None
    rider.stops = json.dumps(stops) if stops else None
    rider.ride_code = ride_code
    rider.owner = owner
    rider.status = status

    # No longer storing pickup_latitude/longitude as separate columns
    # They are part of the 'source' JSON string.

    db.session.commit()

    return jsonify({
        'success': True,
        'message': 'Ride info stored',
        'rider': {
            'userName': rider.userName,
            'pickup': json.loads(rider.source) if rider.source else None,
            'destination': json.loads(rider.destination) if rider.destination else None,
            'stops': json.loads(rider.stops) if rider.stops else [],
            'ride_code': rider.ride_code,
            'owner': rider.owner,
            'status': rider.status,
            # Removed direct pickup_latitude/longitude from response as well
        }
    })

@app.route('/api/trips/<userName>', methods=['GET'])
def get_trip_data_by_username(userName):

    rider = db.session.get(Rider, userName)
    print(userName)
    if not rider:
        return jsonify({
            'pickup': None,
            'destination': None,
            'stops': [],
            'ride_code': None
        }), 404

    pickup_data = json.loads(rider.source) if rider.source else None
    destination_data = json.loads(rider.destination) if rider.destination else None
    stops_data = json.loads(rider.stops) if rider.stops else []

    formatted_pickup = None
    if pickup_data and 'latitude' in pickup_data and 'longitude' in pickup_data:
        formatted_pickup = {
            'latitude': float(pickup_data['latitude']),
            'longitude': float(pickup_data['longitude']),
            'name': pickup_data.get('name')
        }

    formatted_destination = None
    if destination_data and 'latitude' in destination_data and 'longitude' in destination_data:
        formatted_destination = {
            'latitude': float(destination_data['latitude']),
            'longitude': float(destination_data['longitude']),
            'name': destination_data.get('name')
        }

    formatted_stops = []
    for stop in stops_data:
        if isinstance(stop, dict) and 'latitude' in stop and 'longitude' in stop:
            formatted_stops.append({
                'latitude': float(stop['latitude']),
                'longitude': float(stop['longitude']),
                'name': stop.get('name')
            })

    return jsonify({
        'pickup': formatted_pickup,
        'destination': formatted_destination,
        'stops': formatted_stops,
        'ride_code': rider.ride_code
    })

@app.route('/api/ride/<ride_code>', methods=['GET'])
def get_ride_by_code(ride_code):
    rider = Rider.query.filter_by(ride_code=ride_code).first()
    if not rider:
        return jsonify({'error': 'Ride not found'}), 404

    destination = None
    if rider.destination:
        try:
            dest_data = json.loads(rider.destination)
            destination = {
                'latitude': dest_data.get('latitude'),
                'longitude': dest_data.get('longitude')
            }
        except Exception:
            destination = None

    stops = []
    if rider.stops:
        try:
            stops = json.loads(rider.stops)
        except Exception:
            stops = []

    return jsonify({
        'destination': destination,
        'stops': stops
    })

@app.route('/api/update-ride-status/<userName>', methods=['POST'])
def update_ride_status(userName):

    data = request.get_json()
    status = data.get('status')

    if not status:
        return jsonify({'success': False, 'message': 'status is required'}), 400

    rider = db.session.get(Rider, userName)
    if not rider:
        return jsonify({'success': False, 'message': 'Rider not found'}), 404

    try:
        if status == 'inactive':
            # Only update the status to 'inactive'
            rider.status = 'inactive'
            db.session.commit()
            return jsonify({
                'success': True,
                'message': f'Rider {userName} status updated to inactive'
            }), 200
        elif status == 'done':
            # Set all relevant columns to null except userName
            rider.ride_code = None
            rider.source = None
            rider.destination = None
            rider.stops = None
            rider.distance_travelled = 0.0 # Reset to default or None as per preference
            rider.average_speed = 0.0    # Reset to default or None as per preference
            rider.owner = None
            rider.status = 'done' # Set status to 'done'
            rider.current_latitude = None
            rider.current_longitude = None
            rider.expo_push_token = None # Clear push token if ride is done

            db.session.commit()
            return jsonify({
                'success': True,
                'message': f'Rider {userName} ride marked as done and details cleared'
            }), 200
        else:
            rider.status = 'active'
            db.session.commit()
            return jsonify({
                'success': True,
                'message': f'Rider {userName} status updated to active'
            }), 200

    except Exception as e:
        db.session.rollback()
        print(f"Error updating rider status for {userName}: {e}")
        return jsonify({'success': False, 'message': 'An error occurred while updating rider status'}), 500

@app.route('/api/riders/by_user/<username>', methods=['GET'])
def get_riders_by_username(username):
    requesting_rider = db.session.get(Rider, username)
    if not requesting_rider:
        return jsonify({'message': 'Requesting rider not found'}), 404

    if not requesting_rider.ride_code:
        return jsonify({'message': 'Requesting rider is not part of a ride (no ride_code assigned)'}), 400

    active_riders_on_same_ride = Rider.query.filter(
        Rider.ride_code == requesting_rider.ride_code,
        Rider.userName != username,
        Rider.status == 'active'
    ).all()

    riders_data = []
    for rider in active_riders_on_same_ride:
        riders_data.append({
            'id': rider.userName,
            'name': rider.userName,
            'status': rider.status
        })

    return jsonify({'riders': riders_data})

# NEW ENDPOINT: Fetch co-rider pickup locations based on requesting user's ride
@app.route('/api/ride/coworkers-pickup-locations/<username>', methods=['GET'])
def get_coworkers_pickup_locations(username):
    requesting_username = username

    requesting_rider = db.session.get(Rider, requesting_username)
    print(requesting_rider)
    if not requesting_rider:
        return jsonify({"message": "Requesting rider not found."}), 404

    if not requesting_rider.ride_code:
        return jsonify({"message": "Requesting rider is not part of an active ride."}), 400

    active_ride_code = requesting_rider.ride_code

    # Query for all other riders on the same ride
    coworkers = Rider.query.filter(
        Rider.ride_code == active_ride_code,
        Rider.userName != requesting_username,
        Rider.status == 'active'
    ).all()

    locations = []
    for coworker in coworkers:
        # Safely parse the 'source' JSON to get latitude and longitude
        if coworker.source:
            try:
                source_data = json.loads(coworker.source)
                if 'latitude' in source_data and 'longitude' in source_data:
                    locations.append({
                        "latitude": float(source_data['latitude']),
                        "longitude": float(source_data['longitude']),
                        "username": coworker.userName # Include username for marker title
                    })
            except json.JSONDecodeError:
                print(f"Warning: Could not decode source JSON for rider {coworker.userName}")
            except KeyError:
                print(f"Warning: 'latitude' or 'longitude' missing in source for rider {coworker.userName}")


    return jsonify({"coworker_pickup_locations": locations}), 200

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)