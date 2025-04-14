"""
Webhook handling module for AmoCRM events
"""
from flask import Flask, request, jsonify
from typing import Dict, Any, Callable

import config
from logger import log_event
from storage import Storage
from api import AmoCRMAPI

app = Flask(__name__)
storage = None
api = None

def init_webhook(storage_instance: Storage, api_instance: AmoCRMAPI):
    """Initialize webhook module with storage and API instances"""
    global storage, api
    storage = storage_instance
    api = api_instance

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Handle incoming webhook from AmoCRM"""
    try:
        # Get the JSON data from the request
        webhook_data = request.json

        if not webhook_data:
            log_event('webhook', 'error', 'Received empty webhook data')
            return jsonify({'status': 'error', 'message': 'Empty data'}), 400

        log_event('webhook', 'info', 'Received webhook', {'data': webhook_data})

        # Process the webhook data
        process_webhook(webhook_data)

        return jsonify({'status': 'success'}), 200

    except Exception as e:
        log_event('webhook', 'error', f'Error processing webhook: {e}')
        return jsonify({'status': 'error', 'message': str(e)}), 500

def process_webhook(webhook_data: Dict[str, Any]):
    """Process webhook data and update the appropriate entities"""
    # Extract the entity type, ID, and action from the webhook data
    # The exact structure depends on the AmoCRM webhook format

    # Example webhook data structure (adapt as needed):
    # {
    #   "leads": {"status": [{"id": 123, "status_id": 456, ...}], "add": [...], ...},
    #   "contacts": {"update": [{"id": 789, ...}], ...},
    #   ...
    # }

    # Process leads/deals
    if 'leads' in webhook_data:
        process_entity_webhook(webhook_data['leads'], 'leads')

    # Process contacts
    if 'contacts' in webhook_data:
        process_entity_webhook(webhook_data['contacts'], 'contacts')

    # Process companies
    if 'companies' in webhook_data:
        process_entity_webhook(webhook_data['companies'], 'companies')

    # Process custom fields
    if 'custom_fields' in webhook_data:
        process_custom_fields(webhook_data['custom_fields'])

    # Log the webhook event
    add_event_for_webhook(webhook_data)

def process_entity_webhook(entity_data: Dict[str, Any], entity_type: str):
    """Process webhook data for a specific entity type"""
    # Check for added entities
    if 'add' in entity_data and entity_data['add']:
        for entity in entity_data['add']:
            entity_id = entity.get('id')
            if entity_id:
                # Get the full entity data from the API
                full_entity = api.get_entity_by_id(entity_type, entity_id)
                if full_entity:
                    # Update the entity in storage
                    storage.update_entity(entity_type, entity_id, full_entity)

                    # Log the event
                    log_event('webhook', 'info',
                             f'Added {entity_type} with ID {entity_id}')

    # Check for updated entities
    update_keys = [k for k in entity_data.keys() if k != 'add' and k != 'delete']
    for key in update_keys:
        for entity in entity_data.get(key, []):
            entity_id = entity.get('id')
            if entity_id:
                # Get the full entity data from the API
                full_entity = api.get_entity_by_id(entity_type, entity_id)
                if full_entity:
                    # Update the entity in storage
                    storage.update_entity(entity_type, entity_id, full_entity)

                    # Log the event
                    log_event('webhook', 'info',
                             f'Updated {entity_type} with ID {entity_id}')

    # Check for deleted entities
    if 'delete' in entity_data and entity_data['delete']:
        for entity in entity_data['delete']:
            entity_id = entity.get('id')
            if entity_id:
                # Delete the entity from storage
                storage.delete_entity(entity_type, entity_id)

                # Log the event
                log_event('webhook', 'info',
                         f'Deleted {entity_type} with ID {entity_id}')

def process_custom_fields(custom_fields_data: Dict[str, Any]):
    """Process webhook data for custom fields"""
    # The exact implementation depends on the AmoCRM webhook format for custom fields
    # This is a placeholder that should be adapted to the actual format

    # Example implementation:
    if 'add' in custom_fields_data:
        for field in custom_fields_data['add']:
            log_event('webhook', 'info',
                     f'Added custom field: {field.get("name")}')

    if 'update' in custom_fields_data:
        for field in custom_fields_data['update']:
            log_event('webhook', 'info',
                     f'Updated custom field: {field.get("name")}')

    if 'delete' in custom_fields_data:
        for field in custom_fields_data['delete']:
            log_event('webhook', 'info',
                     f'Deleted custom field: {field.get("name")}')

def add_event_for_webhook(webhook_data: Dict[str, Any]):
    """Add an event entry for the webhook"""
    # Create an event object
    event = {
        'timestamp': webhook_data.get('timestamp', ''),
        'type': 'webhook',
        'data': webhook_data
    }

    # Get existing events
    events = storage.get_entities('events')

    # Add the new event
    events.append(event)

    # Save the updated events
    storage.save_entities('events', events)

def start_webhook_server():
    """Start the Flask server for webhook handling"""
    log_event('webhook', 'info', f'Starting webhook server on {config.WEBHOOK_HOST}:{config.WEBHOOK_PORT}')
    app.run(host=config.WEBHOOK_HOST, port=config.WEBHOOK_PORT)

def register_webhook():
    """Register the webhook URL with AmoCRM"""
    webhook_url = f"http://{config.WEBHOOK_HOST}:{config.WEBHOOK_PORT}/webhook"
    return api.register_webhook(webhook_url)