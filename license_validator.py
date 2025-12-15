import logging
from typing import Dict, Any, List

# Define the single, open-source tier with unlimited printers and all features
LICENSE_TIERS = {
    'FREE': {
        'max_printers': -1,  # Unlimited
        'features': ['basic_printing', 'job_queue', 'advanced_reporting', 'email_notifications', 'priority_support', 'api_access', 'custom_branding', 'multi_tenant'] # All features enabled
    }
}

def validate_license() -> Dict[str, Any]:
    """Always returns a valid FREE tier license with unlimited access."""
    logging.info("Using open-source license: FREE tier with unlimited features and printers.")
    
    return {
        'valid': True,
        'tier': 'FREE',
        'customer': 'Open Source User',
        'expires_at': 'Never',
        'last_verified': 'N/A',
        'features': LICENSE_TIERS['FREE']['features'],
        'max_printers': -1,
        'message': 'Open Source - All features enabled.'
    }

def get_license_info() -> Dict[str, Any]:
    """Get complete license information."""
    return validate_license()

def update_license(license_key: str) -> Dict[str, Any]:
    """Stubbed function: Does nothing on update."""
    logging.info(f"License key update requested: {license_key}. Ignoring in open-source version.")
    return validate_license()

def verify_license_startup() -> Dict[str, Any]:
    """Function specifically for startup validation."""
    return validate_license()

def is_feature_enabled(feature_name: str) -> bool:
    """All features are enabled in the open-source version."""
    return True

def get_max_printers() -> int:
    """Returns -1 for unlimited printers."""
    return -1