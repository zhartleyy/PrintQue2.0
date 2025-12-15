# license_routes.py
# Removed all proprietary license routing.

from flask import Blueprint, redirect, url_for, flash, jsonify
import logging

# Define a minimal, empty blueprint just to satisfy register_license_routes
license_bp = Blueprint('license', __name__, url_prefix='/license')

@license_bp.route('/', methods=['GET'])
def license_page():
    """Redirect to main dashboard since license page is removed."""
    flash("This license page is deprecated. PrintQue is now open-source under the MIT license.", "info")
    return redirect(url_for('index'))

@license_bp.route('/update', methods=['POST'])
def update_license_route():
    """Stub: Redirect on update attempt."""
    flash("License update is deprecated. PrintQue is now open-source.", "info")
    return redirect(url_for('license.license_page'))

@license_bp.route('/status', methods=['GET'])
def license_status():
    """Stub API endpoint to get license status."""
    return jsonify({
        'valid': True,
        'tier': 'FREE',
        'message': 'Open Source - All features enabled.'
    })

# Helper function for blueprint registration
def register_license_routes(app, socketio):
    logging.info("Registering stub license routes (open-source version).")
    app.register_blueprint(license_bp)