"""
Command-line interface for Google Sheets setup wizard
"""

import sys
import argparse
import json
from typing import Optional

from ..utils.setup_wizard import GoogleSheetsSetupWizard
from ..core.logger import log_event


def run_setup_wizard_cli():
    """Main CLI function for setup wizard"""
    parser = argparse.ArgumentParser(
        description="Google Sheets Setup Wizard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m amocrm_exporter.cli.setup_wizard_cli --interactive
  python -m amocrm_exporter.cli.setup_wizard_cli --status
  python -m amocrm_exporter.cli.setup_wizard_cli --step 2
  python -m amocrm_exporter.cli.setup_wizard_cli --report
  python -m amocrm_exporter.cli.setup_wizard_cli --troubleshooting
        """
    )

    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Run interactive setup wizard'
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current setup status'
    )

    parser.add_argument(
        '--step',
        type=int,
        help='Show detailed guide for specific step number'
    )

    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate comprehensive setup report'
    )

    parser.add_argument(
        '--troubleshooting',
        action='store_true',
        help='Show troubleshooting guide'
    )

    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results in JSON format'
    )

    parser.add_argument(
        '--open-links',
        type=int,
        help='Open helpful links for specific step in browser'
    )

    parser.add_argument(
        '--validate-step',
        type=int,
        help='Validate a specific step'
    )

    parser.add_argument(
        '--output',
        type=str,
        help='Output file path for results'
    )

    args = parser.parse_args()

    # Initialize setup wizard
    wizard = GoogleSheetsSetupWizard()

    try:
        if args.interactive:
            run_interactive_wizard(wizard)
        elif args.status:
            show_setup_status(wizard, args.json)
        elif args.step:
            show_step_guide(wizard, args.step, args.json)
        elif args.report:
            show_setup_report(wizard, args.output)
        elif args.troubleshooting:
            show_troubleshooting_guide(wizard, args.json)
        elif args.open_links is not None:
            open_step_links(wizard, args.open_links)
        elif args.validate_step:
            validate_specific_step(wizard, args.validate_step, args.json)
        else:
            # Default: show status
            show_setup_status(wizard, args.json)

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


def run_interactive_wizard(wizard: GoogleSheetsSetupWizard):
    """Run interactive setup wizard"""
    print("=" * 60)
    print("GOOGLE SHEETS SETUP WIZARD")
    print("=" * 60)
    print()

    progress = wizard.get_setup_progress()

    print(f"Setup Progress: {progress.overall_progress_percentage:.1f}%")
    print(f"Completed Steps: {len(progress.completed_steps)}/{progress.total_steps}")
    print(f"Current Step: {progress.current_step}")
    print()

    if progress.overall_progress_percentage == 100:
        print("🎉 Setup is complete! You can now use Google Sheets export.")
        return

    # Show current step
    current_step = wizard.get_step_details(progress.current_step)
    if current_step:
        print(f"CURRENT STEP: {current_step.title}")
        print(f"Description: {current_step.description}")
        print()

        if current_step.error_message:
            print(f"❌ Error: {current_step.error_message}")
            print()

        print("Instructions:")
        for i, instruction in enumerate(current_step.instructions, 1):
            print(f"  {i}. {instruction}")
        print()

        # Interactive options
        while True:
            print("Options:")
            print("  1. Validate this step")
            print("  2. Open helpful links")
            print("  3. Show troubleshooting guide")
            print("  4. Skip to next step")
            print("  5. Exit")
            print()

            try:
                choice = input("Choose an option (1-5): ").strip()

                if choice == '1':
                    validate_current_step(wizard, current_step.step_number)
                elif choice == '2':
                    wizard.open_helpful_links(current_step.step_number)
                    print("Opened helpful links in your browser.")
                elif choice == '3':
                    show_troubleshooting_guide(wizard, json_format=False)
                elif choice == '4':
                    print("Moving to next step...")
                    break
                elif choice == '5':
                    print("Exiting setup wizard.")
                    return
                else:
                    print("Invalid choice. Please enter 1-5.")

                print()

            except KeyboardInterrupt:
                print("\nExiting setup wizard.")
                return


def validate_current_step(wizard: GoogleSheetsSetupWizard, step_number: int):
    """Validate the current step"""
    step = wizard.get_step_details(step_number)
    if not step or not step.validation_function:
        print("This step cannot be automatically validated.")
        return

    print(f"Validating Step {step_number}: {step.title}...")

    try:
        validation_method = getattr(wizard, step.validation_function)
        is_valid, error_message = validation_method()

        if is_valid:
            print("✅ Step validation passed!")
            if error_message:  # Warning message
                print(f"⚠️  Warning: {error_message}")
        else:
            print("❌ Step validation failed!")
            if error_message:
                print(f"Error: {error_message}")

    except Exception as e:
        print(f"❌ Validation error: {str(e)}")


def show_setup_status(wizard: GoogleSheetsSetupWizard, json_format: bool = False):
    """Show current setup status"""
    progress = wizard.get_setup_progress()
    steps = wizard.get_all_steps()

    if json_format:
        status_data = {
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
                    "error_message": step.error_message
                }
                for step in steps
            ],
            "warnings": progress.warnings
        }
        print(json.dumps(status_data, indent=2))
    else:
        print("Google Sheets Setup Status")
        print("=" * 30)
        print(f"Progress: {progress.overall_progress_percentage:.1f}%")
        print(f"Completed: {len(progress.completed_steps)}/{progress.total_steps} steps")
        print(f"Current Step: {progress.current_step}")
        print(f"Next Action: {progress.next_action}")

        if progress.estimated_time_remaining_minutes > 0:
            print(f"Estimated Time Remaining: {progress.estimated_time_remaining_minutes} minutes")

        print()
        print("Steps:")
        for step in steps:
            status_icon = "✅" if step.completed else ("❌" if step.error_message else "⏳")
            print(f"  {status_icon} {step.step_number}. {step.title}")
            if step.error_message:
                print(f"      Error: {step.error_message}")

        if progress.warnings:
            print()
            print("Warnings:")
            for warning in progress.warnings:
                print(f"  ⚠️  {warning}")


def show_step_guide(wizard: GoogleSheetsSetupWizard, step_number: int, json_format: bool = False):
    """Show detailed guide for a specific step"""
    guide = wizard.get_step_by_step_guide(step_number)

    if "error" in guide:
        print(f"Error: {guide['error']}")
        return

    if json_format:
        print(json.dumps(guide, indent=2))
    else:
        step = guide["step"]
        print(f"Step {step['number']}: {step['title']}")
        print("=" * 50)
        print(f"Description: {step['description']}")
        print(f"Status: {'✅ Completed' if step['completed'] else '⏳ Pending'}")
        print(f"Estimated Time: {guide['estimated_time_minutes']} minutes")
        print(f"Difficulty: {guide['difficulty_level']}")

        if step['error_message']:
            print(f"Error: {step['error_message']}")

        print()

        # Prerequisites
        if guide["prerequisites"]["required"]:
            print("Prerequisites:")
            for prereq in guide["prerequisites"]["required"]:
                print(f"  - Step {prereq}")

            if guide["prerequisites"]["issues"]:
                print("  ❌ Incomplete prerequisites:")
                for issue in guide["prerequisites"]["issues"]:
                    print(f"    - {issue}")
            else:
                print("  ✅ All prerequisites completed")
            print()

        # Instructions
        print("Instructions:")
        for i, instruction in enumerate(guide["instructions"], 1):
            print(f"  {i}. {instruction}")
        print()

        # Help links
        if guide["help_links"]:
            print("Helpful Links:")
            for link in guide["help_links"]:
                print(f"  - {link['title']}: {link['url']}")
            print()

        # Validation info
        if guide["validation"]["can_validate"]:
            print("💡 This step can be automatically validated.")
            print(f"   Run: python -m amocrm_exporter.cli.setup_wizard_cli --validate-step {step_number}")


def show_setup_report(wizard: GoogleSheetsSetupWizard, output_file: Optional[str] = None):
    """Show comprehensive setup report"""
    report = wizard.generate_setup_report()

    if output_file:
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"Setup report saved to: {output_file}")
    else:
        print(report)


def show_troubleshooting_guide(wizard: GoogleSheetsSetupWizard, json_format: bool = False):
    """Show troubleshooting guide"""
    guide = wizard.get_troubleshooting_guide()

    if json_format:
        print(json.dumps(guide, indent=2))
    else:
        print("Google Sheets Setup Troubleshooting Guide")
        print("=" * 45)
        print()

        print("COMMON PROBLEMS:")
        for i, problem in enumerate(guide["common_problems"], 1):
            print(f"{i}. {problem['problem']}")
            print("   Symptoms:")
            for symptom in problem['symptoms']:
                print(f"     - {symptom}")
            print("   Solutions:")
            for solution in problem['solutions']:
                print(f"     - {solution}")
            if problem.get('help_links'):
                print("   Help Links:")
                for link in problem['help_links']:
                    print(f"     - {link['title']}: {link['url']}")
            print()

        print("DIAGNOSTIC COMMANDS:")
        for cmd in guide["diagnostic_commands"]:
            print(f"  {cmd['command']}")
            print(f"    {cmd['description']}")
        print()

        print("HELPFUL LINKS:")
        for link in guide["helpful_links"]:
            print(f"  - {link['title']}: {link['url']}")
            print(f"    {link['description']}")


def open_step_links(wizard: GoogleSheetsSetupWizard, step_number: int):
    """Open helpful links for a specific step"""
    try:
        wizard.open_helpful_links(step_number)
        print(f"Opened helpful links for Step {step_number} in your browser.")
    except Exception as e:
        print(f"Error opening links: {str(e)}")


def validate_specific_step(wizard: GoogleSheetsSetupWizard, step_number: int, json_format: bool = False):
    """Validate a specific step"""
    step = wizard.get_step_details(step_number)

    if not step:
        print(f"Error: Step {step_number} not found.")
        return

    if not step.validation_function:
        result = {
            "step_number": step_number,
            "can_validate": False,
            "message": "This step cannot be automatically validated."
        }
    else:
        try:
            validation_method = getattr(wizard, step.validation_function)
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

    if json_format:
        print(json.dumps(result, indent=2))
    else:
        print(f"Step {step_number} Validation: {step.title}")
        print("=" * 40)

        if not result["can_validate"]:
            print("❓ This step cannot be automatically validated.")
        elif result["is_valid"]:
            print("✅ Validation passed!")
            if result["error_message"]:
                print(f"⚠️  Warning: {result['error_message']}")
        else:
            print("❌ Validation failed!")
            if result["error_message"]:
                print(f"Error: {result['error_message']}")


if __name__ == '__main__':
    run_setup_wizard_cli()