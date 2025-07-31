"""
Command-line interface for Google Sheets configuration validation
"""

import sys
import argparse
import json
from typing import Optional

from ..utils.config_validator import ConfigurationValidator
from ..core.logger import log_event


def run_validation_cli():
    """Main CLI function for configuration validation"""
    parser = argparse.ArgumentParser(
        description="Google Sheets Configuration Validator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m amocrm_exporter.cli.config_validator_cli --report
  python -m amocrm_exporter.cli.config_validator_cli --json
  python -m amocrm_exporter.cli.config_validator_cli --test connection
  python -m amocrm_exporter.cli.config_validator_cli --fix-suggestions
        """
    )

    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate a comprehensive diagnostic report'
    )

    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results in JSON format'
    )

    parser.add_argument(
        '--test',
        choices=['basic', 'connection', 'oauth', 'diagnostics', 'all'],
        help='Run specific test type'
    )

    parser.add_argument(
        '--fix-suggestions',
        action='store_true',
        help='Show fix suggestions for failed tests'
    )

    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress verbose output'
    )

    parser.add_argument(
        '--output',
        type=str,
        help='Output file path for results'
    )

    args = parser.parse_args()

    # Initialize validator
    validator = ConfigurationValidator()

    try:
        if args.test:
            # Run specific test
            results = run_specific_test(validator, args.test, args.quiet)
        else:
            # Run comprehensive validation
            if not args.quiet:
                print("Running comprehensive Google Sheets configuration validation...")
            results = validator.run_comprehensive_validation()

        # Output results
        if args.report:
            output_content = validator.get_diagnostic_report()
            print(output_content)
        elif args.json:
            output_content = json.dumps(results, indent=2, default=str)
            print(output_content)
        else:
            output_content = format_validation_results(results, args.fix_suggestions)
            print(output_content)

        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output_content)
            if not args.quiet:
                print(f"\nResults saved to: {args.output}")

        # Exit with appropriate code
        overall_status = results.get('overall_status', 'unknown')
        if overall_status == 'ready':
            sys.exit(0)
        elif overall_status in ['configured_with_warnings', 'needs_configuration']:
            sys.exit(1)
        else:
            sys.exit(2)

    except Exception as e:
        print(f"Error during validation: {str(e)}", file=sys.stderr)
        sys.exit(3)


def run_specific_test(validator: ConfigurationValidator, test_type: str, quiet: bool = False) -> dict:
    """Run a specific type of test"""
    if not quiet:
        print(f"Running {test_type} test...")

    if test_type == 'basic':
        basic_result = validator._run_basic_validation()
        return {
            'test_type': 'basic',
            'timestamp': validator.run_comprehensive_validation()['timestamp'],
            'result': basic_result
        }

    elif test_type == 'connection':
        connection_result = validator._test_google_sheets_connection()
        return {
            'test_type': 'connection',
            'timestamp': validator.run_comprehensive_validation()['timestamp'],
            'result': connection_result.__dict__
        }

    elif test_type == 'oauth':
        oauth_result = validator._validate_oauth_flow()
        return {
            'test_type': 'oauth',
            'timestamp': validator.run_comprehensive_validation()['timestamp'],
            'result': oauth_result.__dict__
        }

    elif test_type == 'diagnostics':
        diagnostic_results = validator._run_diagnostic_tests()
        return {
            'test_type': 'diagnostics',
            'timestamp': validator.run_comprehensive_validation()['timestamp'],
            'result': [test.__dict__ for test in diagnostic_results]
        }

    else:  # 'all'
        return validator.run_comprehensive_validation()


def format_validation_results(results: dict, show_fix_suggestions: bool = False) -> str:
    """Format validation results for human-readable output"""
    lines = []

    # Header
    lines.append("Google Sheets Configuration Validation Results")
    lines.append("=" * 50)
    lines.append(f"Status: {results.get('overall_status', 'unknown').upper()}")
    lines.append("")

    # Summary
    summary = results.get('summary', {})
    if summary:
        lines.append("Summary:")
        lines.append(f"  Tests Run: {summary.get('total_tests', 0)}")
        lines.append(f"  Passed: {summary.get('passed_tests', 0)}")
        lines.append(f"  Failed: {summary.get('failed_tests', 0)}")
        lines.append(f"  Warnings: {summary.get('warnings', 0)}")
        lines.append(f"  Ready for Export: {'Yes' if summary.get('ready_for_export') else 'No'}")
        lines.append("")

    # Basic validation
    basic_validation = results.get('basic_validation', {})
    if basic_validation:
        status = "PASS" if basic_validation.get('is_valid') else "FAIL"
        lines.append(f"Basic Configuration: {status}")

        if basic_validation.get('errors'):
            lines.append("  Errors:")
            for error in basic_validation['errors']:
                lines.append(f"    • {error}")

        if basic_validation.get('warnings'):
            lines.append("  Warnings:")
            for warning in basic_validation['warnings']:
                lines.append(f"    • {warning}")

        lines.append("")

    # Connection test
    connection_test = results.get('connection_test')
    if connection_test:
        status = "PASS" if connection_test.get('can_connect') else "FAIL"
        lines.append(f"Connection Test: {status}")

        if connection_test.get('error_details'):
            lines.append(f"  Error: {connection_test['error_details']}")

        if connection_test.get('response_time_ms'):
            lines.append(f"  Response Time: {connection_test['response_time_ms']:.0f}ms")

        lines.append("")

    # OAuth validation
    oauth_validation = results.get('oauth_validation')
    if oauth_validation:
        creds_status = "Valid" if oauth_validation.get('credentials_file_valid') else "Invalid"
        token_status = "Valid" if oauth_validation.get('token_valid') else "Invalid"

        lines.append(f"OAuth Validation:")
        lines.append(f"  Credentials File: {creds_status}")
        lines.append(f"  Token: {token_status}")

        if oauth_validation.get('expires_at'):
            lines.append(f"  Token Expires: {oauth_validation['expires_at']}")

        lines.append("")

    # Diagnostic tests
    diagnostic_tests = results.get('diagnostic_tests', [])
    if diagnostic_tests:
        lines.append("Diagnostic Tests:")
        for test in diagnostic_tests:
            status = "PASS" if test.get('passed') else "FAIL"
            lines.append(f"  {test.get('test_name', 'Unknown')}: {status}")

            if test.get('message'):
                lines.append(f"    {test['message']}")

            if show_fix_suggestions and test.get('fix_suggestions') and not test.get('passed'):
                lines.append("    Fix suggestions:")
                for suggestion in test['fix_suggestions']:
                    lines.append(f"      - {suggestion}")

        lines.append("")

    # Recommendations
    recommendations = results.get('recommendations', [])
    if recommendations:
        lines.append("Recommendations:")
        for recommendation in recommendations:
            lines.append(f"  • {recommendation}")
        lines.append("")

    return "\n".join(lines)


if __name__ == '__main__':
    run_validation_cli()