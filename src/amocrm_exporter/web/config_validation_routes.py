"""
FastAPI routes for Google Sheets configuration validation
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, PlainTextResponse
from typing import Dict, Any, Optional
import asyncio
from datetime import datetime

from ..utils.config_validator import ConfigurationValidator
from ..utils.setup_wizard import GoogleSheetsSetupWizard
from ..core.logger import log_event

router = APIRouter(prefix="/api/config", tags=["configuration"])

# Global validator and wizard instances
validator = ConfigurationValidator()
setup_wizard = GoogleSheetsSetupWizard()

# Cache for validation results (simple in-memory cache)
_validation_cache = {
    'last_validation': None,
    'cache_time': None,
    'cache_ttl_minutes': 5
}


@router.get("/validate")
async def validate_configuration(
    force_refresh: bool = False,
    test_type: Optional[str] = None
) -> JSONResponse:
    """
    Validate Google Sheets configuration

    Args:
        force_refresh: Force a new validation instead of using cached results
        test_type: Specific test type to run ('basic', 'connection', 'oauth', 'diagnostics')

    Returns:
        JSON response with validation results
    """
    try:
        # Check cache if not forcing refresh
        if not force_refresh and _is_cache_valid():
            log_event("config_validation", "info", "Returning cached validation results")
            return JSONResponse(content=_validation_cache['last_validation'])

        log_event("config_validation", "info", f"Running configuration validation (test_type: {test_type})")

        # Run validation based on test type
        if test_type == 'basic':
            result = {
                'test_type': 'basic',
                'timestamp': datetime.now().isoformat(),
                'basic_validation': validator._run_basic_validation(),
                'overall_status': 'basic_only'
            }
        elif test_type == 'connection':
            connection_result = validator._test_google_sheets_connection()
            result = {
                'test_type': 'connection',
                'timestamp': datetime.now().isoformat(),
                'connection_test': connection_result.__dict__,
                'overall_status': 'ready' if connection_result.can_connect else 'connection_failed'
            }
        elif test_type == 'oauth':
            oauth_result = validator._validate_oauth_flow()
            result = {
                'test_type': 'oauth',
                'timestamp': datetime.now().isoformat(),
                'oauth_validation': oauth_result.__dict__,
                'overall_status': 'ready' if oauth_result.token_valid else 'oauth_failed'
            }
        elif test_type == 'diagnostics':
            diagnostic_results = validator._run_diagnostic_tests()
            result = {
                'test_type': 'diagnostics',
                'timestamp': datetime.now().isoformat(),
                'diagnostic_tests': [test.__dict__ for test in diagnostic_results],
                'overall_status': 'diagnostics_complete'
            }
        else:
            # Run comprehensive validation
            result = validator.run_comprehensive_validation()

        # Cache the results
        _validation_cache['last_validation'] = result
        _validation_cache['cache_time'] = datetime.now()

        return JSONResponse(content=result)

    except Exception as e:
        log_event("config_validation", "error", f"Error during configuration validation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


@router.get("/validate/report")
async def get_validation_report(force_refresh: bool = False) -> PlainTextResponse:
    """
    Get a human-readable diagnostic report

    Args:
        force_refresh: Force a new validation instead of using cached results

    Returns:
        Plain text diagnostic report
    """
    try:
        log_event("config_validation", "info", "Generating diagnostic report")

        # Generate report (this will run validation if needed)
        report = validator.get_diagnostic_report()

        return PlainTextResponse(content=report, media_type="text/plain")

    except Exception as e:
        log_event("config_validation", "error", f"Error generating diagnostic report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")


@router.get("/status")
async def get_configuration_status() -> JSONResponse:
    """
    Get a quick status check of the configuration

    Returns:
        JSON response with basic status information
    """
    try:
        # Get configuration summary from the config manager
        summary = validator.config_manager.get_configuration_summary()

        # Add quick status assessment
        status = "unknown"
        if summary.get('validation_status', {}).get('is_valid'):
            status = "ready"
        elif summary.get('credentials_file_exists') and summary.get('token_file_exists'):
            status = "configured"
        elif summary.get('credentials_file_exists'):
            status = "needs_authentication"
        else:
            status = "needs_setup"

        result = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'summary': summary
        }

        return JSONResponse(content=result)

    except Exception as e:
        log_event("config_validation", "error", f"Error getting configuration status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")


@router.get("/setup-guide")
async def get_setup_guide() -> JSONResponse:
    """
    Get guided setup assistance

    Returns:
        JSON response with setup instructions and current status
    """
    try:
        log_event("config_validation", "info", "Generating setup guide")

        setup_guide = validator.config_manager.setup_guided_configuration()

        return JSONResponse(content=setup_guide)

    except Exception as e:
        log_event("config_validation", "error", f"Error generating setup guide: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Setup guide generation failed: {str(e)}")


@router.post("/test-spreadsheet/{entity_type}")
async def test_spreadsheet_access(entity_type: str) -> JSONResponse:
    """
    Test access to a specific spreadsheet

    Args:
        entity_type: The entity type to test ('leads', 'contacts', 'companies', 'events')

    Returns:
        JSON response with test results
    """
    try:
        if entity_type not in ['leads', 'contacts', 'companies', 'events']:
            raise HTTPException(status_code=400, detail="Invalid entity type")

        log_event("config_validation", "info", f"Testing {entity_type} spreadsheet access")

        # Get spreadsheet ID for the entity type
        spreadsheet_ids = validator.config_manager._get_spreadsheet_ids()
        spreadsheet_id = spreadsheet_ids.get(entity_type)

        if not spreadsheet_id:
            return JSONResponse(content={
                'entity_type': entity_type,
                'accessible': False,
                'error': f"No spreadsheet ID configured for {entity_type}",
                'spreadsheet_id': None
            })

        # Test spreadsheet access
        try:
            spreadsheet_info = validator.config_manager.get_spreadsheet_info(spreadsheet_id)
            permission_result = validator.config_manager.test_permissions(spreadsheet_id)

            result = {
                'entity_type': entity_type,
                'accessible': True,
                'spreadsheet_id': spreadsheet_id,
                'spreadsheet_info': {
                    'title': spreadsheet_info.title,
                    'url': spreadsheet_info.url,
                    'sheet_count': len(spreadsheet_info.sheets)
                },
                'permissions': {
                    'can_read': permission_result.can_read,
                    'can_write': permission_result.can_write,
                    'can_create_sheets': permission_result.can_create_sheets
                },
                'error': permission_result.error_message
            }

            return JSONResponse(content=result)

        except Exception as e:
            return JSONResponse(content={
                'entity_type': entity_type,
                'accessible': False,
                'error': str(e),
                'spreadsheet_id': spreadsheet_id
            })

    except Exception as e:
        log_event("config_validation", "error", f"Error testing {entity_type} spreadsheet: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Spreadsheet test failed: {str(e)}")


@router.post("/refresh-token")
async def refresh_oauth_token() -> JSONResponse:
    """
    Refresh the OAuth token

    Returns:
        JSON response with refresh results
    """
    try:
        log_event("config_validation", "info", "Refreshing OAuth token")

        # Try to refresh credentials
        try:
            validator.config_manager._get_credentials()

            # Validate the refreshed credentials
            oauth_result = validator._validate_oauth_flow()

            result = {
                'success': oauth_result.token_valid,
                'token_valid': oauth_result.token_valid,
                'can_refresh': oauth_result.can_refresh,
                'expires_at': oauth_result.expires_at.isoformat() if oauth_result.expires_at else None,
                'message': "Token refreshed successfully" if oauth_result.token_valid else "Token refresh failed"
            }

            # Clear validation cache since credentials changed
            _validation_cache['last_validation'] = None
            _validation_cache['cache_time'] = None

            return JSONResponse(content=result)

        except Exception as e:
            return JSONResponse(content={
                'success': False,
                'token_valid': False,
                'can_refresh': False,
                'message': f"Token refresh failed: {str(e)}"
            })

    except Exception as e:
        log_event("config_validation", "error", f"Error refreshing OAuth token: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Token refresh failed: {str(e)}")


@router.get("/diagnostics/{test_name}")
async def run_specific_diagnostic(test_name: str) -> JSONResponse:
    """
    Run a specific diagnostic test

    Args:
        test_name: Name of the diagnostic test to run

    Returns:
        JSON response with test results
    """
    try:
        valid_tests = [
            'file_permissions', 'network_connectivity', 'environment_variables',
            'spreadsheet_formats', 'api_quotas', 'credential_file_format'
        ]

        if test_name not in valid_tests:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid test name. Valid tests: {', '.join(valid_tests)}"
            )

        log_event("config_validation", "info", f"Running specific diagnostic test: {test_name}")

        # Map test names to methods
        test_methods = {
            'file_permissions': validator._test_file_permissions,
            'network_connectivity': validator._test_network_connectivity,
            'environment_variables': validator._test_environment_variables,
            'spreadsheet_formats': validator._test_spreadsheet_formats,
            'api_quotas': validator._test_api_quotas,
            'credential_file_format': validator._test_credential_file_format
        }

        # Run the specific test
        test_result = test_methods[test_name]()

        result = {
            'test_name': test_name,
            'timestamp': datetime.now().isoformat(),
            'result': test_result.__dict__
        }

        return JSONResponse(content=result)

    except Exception as e:
        log_event("config_validation", "error", f"Error running diagnostic test {test_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Diagnostic test failed: {str(e)}")


def _is_cache_valid() -> bool:
    """Check if the validation cache is still valid"""
    if not _validation_cache['last_validation'] or not _validation_cache['cache_time']:
        return False

    cache_age_minutes = (datetime.now() - _validation_cache['cache_time']).total_seconds() / 60
    return cache_age_minutes < _validation_cache['cache_ttl_minutes']


# Setup Wizard Routes

@router.get("/setup/status")
async def get_setup_status() -> JSONResponse:
    """
    Get current setup wizard status

    Returns:
        JSON response with setup progress and step details
    """
    try:
        log_event("setup_wizard", "info", "Getting setup status")

        progress = setup_wizard.get_setup_progress()
        steps = setup_wizard.get_all_steps()

        result = {
            "progress": {
                "current_step": progress.current_step,
                "total_steps": progress.total_steps,
                "completed_steps": progress.completed_steps,
                "failed_steps": progress.failed_steps,
                "progress_percentage": progress.overall_progress_percentage,
                "estimated_time_remaining_minutes": progress.estimated_time_remaining_minutes,
                "next_action": progress.next_action
            },
            "steps": [
                {
                    "step_number": step.step_number,
                    "title": step.title,
                    "description": step.description,
                    "completed": step.completed,
                    "error_message": step.error_message,
                    "prerequisites": step.prerequisites
                }
                for step in steps
            ],
            "warnings": progress.warnings,
            "timestamp": datetime.now().isoformat()
        }

        return JSONResponse(content=result)

    except Exception as e:
        log_event("setup_wizard", "error", f"Error getting setup status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Setup status retrieval failed: {str(e)}")


@router.get("/setup/step/{step_number}")
async def get_step_guide(step_number: int) -> JSONResponse:
    """
    Get detailed guide for a specific setup step

    Args:
        step_number: The step number to get details for

    Returns:
        JSON response with step details and instructions
    """
    try:
        log_event("setup_wizard", "info", f"Getting guide for step {step_number}")

        guide = setup_wizard.get_step_by_step_guide(step_number)

        if "error" in guide:
            raise HTTPException(status_code=404, detail=guide["error"])

        return JSONResponse(content=guide)

    except HTTPException:
        raise
    except Exception as e:
        log_event("setup_wizard", "error", f"Error getting step guide for step {step_number}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Step guide retrieval failed: {str(e)}")


@router.post("/setup/validate-step/{step_number}")
async def validate_setup_step(step_number: int) -> JSONResponse:
    """
    Validate a specific setup step

    Args:
        step_number: The step number to validate

    Returns:
        JSON response with validation results
    """
    try:
        log_event("setup_wizard", "info", f"Validating setup step {step_number}")

        step = setup_wizard.get_step_details(step_number)

        if not step:
            raise HTTPException(status_code=404, detail=f"Step {step_number} not found")

        if not step.validation_function:
            result = {
                "step_number": step_number,
                "step_title": step.title,
                "can_validate": False,
                "message": "This step cannot be automatically validated."
            }
        else:
            try:
                validation_method = getattr(setup_wizard, step.validation_function)
                is_valid, error_message = validation_method()

                result = {
                    "step_number": step_number,
                    "step_title": step.title,
                    "can_validate": True,
                    "is_valid": is_valid,
                    "error_message": error_message,
                    "status": "passed" if is_valid else "failed"
                }

            except Exception as e:
                result = {
                    "step_number": step_number,
                    "step_title": step.title,
                    "can_validate": True,
                    "is_valid": False,
                    "error_message": str(e),
                    "status": "error"
                }

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        log_event("setup_wizard", "error", f"Error validating step {step_number}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Step validation failed: {str(e)}")


@router.get("/setup/troubleshooting")
async def get_troubleshooting_guide() -> JSONResponse:
    """
    Get comprehensive troubleshooting guide

    Returns:
        JSON response with troubleshooting information
    """
    try:
        log_event("setup_wizard", "info", "Getting troubleshooting guide")

        guide = setup_wizard.get_troubleshooting_guide()

        return JSONResponse(content=guide)

    except Exception as e:
        log_event("setup_wizard", "error", f"Error getting troubleshooting guide: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Troubleshooting guide retrieval failed: {str(e)}")


@router.get("/setup/report")
async def get_setup_report() -> PlainTextResponse:
    """
    Get comprehensive setup report in text format

    Returns:
        Plain text setup report
    """
    try:
        log_event("setup_wizard", "info", "Generating setup report")

        report = setup_wizard.generate_setup_report()

        return PlainTextResponse(content=report, media_type="text/plain")

    except Exception as e:
        log_event("setup_wizard", "error", f"Error generating setup report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Setup report generation failed: {str(e)}")


@router.post("/setup/open-links/{step_number}")
async def open_step_links(step_number: int) -> JSONResponse:
    """
    Open helpful links for a specific step (server-side action)

    Args:
        step_number: The step number to open links for

    Returns:
        JSON response with action result
    """
    try:
        log_event("setup_wizard", "info", f"Opening links for step {step_number}")

        step = setup_wizard.get_step_details(step_number)

        if not step:
            raise HTTPException(status_code=404, detail=f"Step {step_number} not found")

        # Get the links that would be opened
        links = step.help_links or []

        # Note: We can't actually open browser links from a web API
        # This endpoint returns the links for the frontend to open
        result = {
            "step_number": step_number,
            "step_title": step.title,
            "links_available": len(links),
            "links": links,
            "message": f"Found {len(links)} helpful links for step {step_number}"
        }

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        log_event("setup_wizard", "error", f"Error getting links for step {step_number}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Link retrieval failed: {str(e)}")


def _is_cache_valid() -> bool:
    """Check if the validation cache is still valid"""
    if not _validation_cache['last_validation'] or not _validation_cache['cache_time']:
        return False

    cache_age_minutes = (datetime.now() - _validation_cache['cache_time']).total_seconds() / 60
    return cache_age_minutes < _validation_cache['cache_ttl_minutes']