"""
Master orchestrator for budget automation.

CLI interface for generating folders, downloading YSL reports, populating templates,
and validating results.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, List
import argparse

from config import TEMPLATE_PATH, OUTPUT_BASE_DIR, YARDI_DOWNLOADS_DIR, YARDI_ENTITY_ID, LOG_FORMAT, LOG_LEVEL
from folder_generator import generate_folders_for_csv, BuildingInfo, FolderGenerator
from ysl_parser import parse_ysl_file
from template_populator import populate_template, TemplatePopulator
from yardi_downloader import YardiDownloader
from validator import BudgetValidator

# Setup logging
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def setup_logging(level: str = LOG_LEVEL) -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        format=LOG_FORMAT,
        level=level,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(OUTPUT_BASE_DIR / "budget_automation.log"),
        ]
    )


class BudgetOrchestrator:
    """Orchestrates budget automation workflow."""

    def __init__(self, template_path: Path = TEMPLATE_PATH,
                 output_base_dir: Path = OUTPUT_BASE_DIR):
        """
        Initialize orchestrator.

        Args:
            template_path: Path to template.xlsx
            output_base_dir: Base output directory
        """
        self.template_path = Path(template_path)
        self.output_base_dir = Path(output_base_dir)
        self.yardi_dir = Path(YARDI_DOWNLOADS_DIR)

    def generate_folders(self, source: Path, source_type: str = "csv") -> bool:
        """
        Generate building folders from source.

        Args:
            source: CSV file or Monday.com API connection
            source_type: "csv" or "monday"

        Returns:
            True if successful
        """
        logger.info(f"Generating folders from {source_type}: {source}")

        if source_type == "csv":
            return generate_folders_for_csv(self.template_path, source, self.output_base_dir)
        elif source_type == "monday":
            # TODO: Implement Monday.com integration
            logger.error("Monday.com integration not yet implemented")
            return False
        else:
            logger.error(f"Unknown source type: {source_type}")
            return False

    def download_ysl_reports(self, building_code: Optional[str] = None,
                            username: Optional[str] = None,
                            password: Optional[str] = None) -> bool:
        """
        Download YSL reports from Yardi.

        Args:
            building_code: Specific building to download (or None for all)
            username: Yardi username
            password: Yardi password

        Returns:
            True if successful
        """
        logger.info("Starting YSL report downloads from Yardi")

        downloader = YardiDownloader(YARDI_ENTITY_ID, username or "", password or "",
                                    self.yardi_dir)

        if not downloader.is_authenticated:
            logger.info("Please provide Yardi credentials")
            if not username:
                username = input("Yardi username: ")
            if not password:
                import getpass
                password = getpass.getpass("Yardi password: ")

            downloader = YardiDownloader(YARDI_ENTITY_ID, username, password, self.yardi_dir)

        # Get list of properties to download
        if building_code:
            property_codes = [building_code]
        else:
            # Scan building folders for properties
            property_codes = self._get_building_codes()

        if not property_codes:
            logger.warning("No buildings found to download")
            return False

        logger.info(f"Downloading YSL for {len(property_codes)} properties")
        success_count = 0

        for prop_code in property_codes:
            result = downloader.download_ysl_for_property(prop_code)
            if result:
                success_count += 1

        logger.info(f"Downloaded YSL for {success_count}/{len(property_codes)} properties")
        return success_count > 0

    def populate_templates(self, building_code: Optional[str] = None) -> bool:
        """
        Populate templates with YSL data.

        Scans building folders' yardi_drops/ for YSL files and populates templates.

        Args:
            building_code: Specific building to populate (or None for all)

        Returns:
            True if successful
        """
        logger.info("Starting template population")

        buildings = self._get_building_folders(building_code)
        if not buildings:
            logger.warning("No buildings found")
            return False

        success_count = 0

        for building_path in buildings:
            if self._populate_building_template(building_path):
                success_count += 1

        logger.info(f"Populated {success_count}/{len(buildings)} templates")
        return success_count > 0

    def validate_templates(self, building_code: Optional[str] = None) -> bool:
        """
        Validate populated templates.

        Args:
            building_code: Specific building to validate (or None for all)

        Returns:
            True if successful
        """
        logger.info("Starting template validation")

        buildings = self._get_building_folders(building_code)
        if not buildings:
            logger.warning("No buildings found")
            return False

        success_count = 0

        for building_path in buildings:
            template_path = self._find_template_in_folder(building_path)
            if template_path:
                if self._validate_template(template_path):
                    success_count += 1

        logger.info(f"Validated {success_count}/{len(buildings)} templates")
        return success_count > 0

    def full_pipeline(self) -> bool:
        """
        Run complete automation pipeline.

        Steps:
        1. Scan for new YSL files in yardi_drops
        2. Populate templates
        3. Validate results

        Returns:
            True if successful
        """
        logger.info("Starting full pipeline")

        # Step 1: Check for YSL files and populate
        if not self.populate_templates():
            logger.warning("No templates populated")
            return False

        # Step 2: Validate
        if not self.validate_templates():
            logger.warning("Validation issues found")
            return False

        logger.info("Pipeline complete")
        return True

    def _get_building_codes(self) -> List[str]:
        """Get list of building codes from folders."""
        codes = []

        if not self.output_base_dir.exists():
            return codes

        for folder in self.output_base_dir.iterdir():
            if folder.is_dir() and " - " in folder.name:
                code = folder.name.split(" - ")[0]
                codes.append(code)

        return sorted(codes)

    def _get_building_folders(self, building_code: Optional[str] = None) -> List[Path]:
        """Get list of building folders."""
        buildings = []

        if not self.output_base_dir.exists():
            return buildings

        for folder in self.output_base_dir.iterdir():
            if not folder.is_dir():
                continue

            if building_code and not folder.name.startswith(f"{building_code} - "):
                continue

            buildings.append(folder)

        return sorted(buildings)

    def _find_template_in_folder(self, folder_path: Path) -> Optional[Path]:
        """Find template file in building folder."""
        for file in folder_path.glob("*_Budget.xlsx"):
            return file

        return None

    def _find_ysl_files_in_drops(self, folder_path: Path) -> List[Path]:
        """Find YSL files in yardi_drops subfolder."""
        ysl_dir = folder_path / "yardi_drops"

        if not ysl_dir.exists():
            return []

        return list(ysl_dir.glob("YSL_*.xlsx"))

    def _populate_building_template(self, building_path: Path) -> bool:
        """Populate template for a building folder."""
        try:
            template_path = self._find_template_in_folder(building_path)
            if not template_path:
                logger.warning(f"No template found in {building_path}")
                return False

            # Find YSL files
            ysl_files = self._find_ysl_files_in_drops(building_path)
            if not ysl_files:
                logger.info(f"No YSL files in {building_path}")
                return False

            # Use the most recent YSL file
            ysl_file = sorted(ysl_files)[-1]
            logger.info(f"Using YSL file: {ysl_file}")

            # Parse YSL
            gl_data, property_info = parse_ysl_file(ysl_file)
            if not gl_data:
                logger.warning(f"No GL data extracted from {ysl_file}")
                return False

            # Populate template
            output_path = template_path.with_stem(template_path.stem + "_filled")
            success = populate_template(
                self.template_path,
                gl_data,
                property_info,
                output_path,
            )

            if success:
                # Replace original with filled version
                import shutil
                shutil.move(str(output_path), str(template_path))
                logger.info(f"Populated {building_path.name}")

            return success

        except Exception as e:
            logger.error(f"Error populating {building_path}: {e}")
            return False

    def _validate_template(self, template_path: Path) -> bool:
        """Validate a template and generate reports."""
        try:
            validator = BudgetValidator(template_path)

            # Generate reports
            report_path = template_path.parent / f"validation_report.txt"
            html_path = template_path.parent / f"validation_report.html"

            validator.generate_report(report_path)
            validator.generate_html_report(html_path)

            logger.info(f"Generated validation reports: {report_path}")
            return True

        except Exception as e:
            logger.error(f"Error validating {template_path}: {e}")
            return False


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Budget automation system orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_budgets.py generate-folders --source buildings.csv
  python run_budgets.py download --all
  python run_budgets.py populate --all
  python run_budgets.py validate --all
  python run_budgets.py full-pipeline
        """,
    )

    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    parser.add_argument("--template", type=Path, default=TEMPLATE_PATH,
                       help="Path to template.xlsx")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_BASE_DIR,
                       help="Output base directory")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # generate-folders command
    gen_parser = subparsers.add_parser("generate-folders", help="Generate building folders")
    gen_parser.add_argument("--source", type=Path, required=True,
                           help="Source file (buildings.csv) or Monday.com config")
    gen_parser.add_argument("--source-type", choices=["csv", "monday"], default="csv",
                           help="Source type")

    # download command
    dl_parser = subparsers.add_parser("download", help="Download YSL reports")
    dl_group = dl_parser.add_mutually_exclusive_group(required=True)
    dl_group.add_argument("--all", action="store_true", help="Download all buildings")
    dl_group.add_argument("--building", type=str, help="Specific building code")
    dl_parser.add_argument("--username", help="Yardi username")
    dl_parser.add_argument("--password", help="Yardi password")

    # populate command
    pop_parser = subparsers.add_parser("populate", help="Populate templates")
    pop_group = pop_parser.add_mutually_exclusive_group(required=True)
    pop_group.add_argument("--all", action="store_true", help="Populate all buildings")
    pop_group.add_argument("--building", type=str, help="Specific building code")

    # validate command
    val_parser = subparsers.add_parser("validate", help="Validate templates")
    val_group = val_parser.add_mutually_exclusive_group(required=True)
    val_group.add_argument("--all", action="store_true", help="Validate all buildings")
    val_group.add_argument("--building", type=str, help="Specific building code")

    # full-pipeline command
    subparsers.add_parser("full-pipeline", help="Run complete pipeline")

    args = parser.parse_args()

    # Setup logging
    level = "DEBUG" if args.verbose else "INFO"
    setup_logging(level)

    # Create orchestrator
    orchestrator = BudgetOrchestrator(args.template, args.output_dir)

    # Execute command
    if args.command == "generate-folders":
        success = orchestrator.generate_folders(args.source, args.source_type)

    elif args.command == "download":
        building = args.building if not args.all else None
        success = orchestrator.download_ysl_reports(building, args.username, args.password)

    elif args.command == "populate":
        building = args.building if not args.all else None
        success = orchestrator.populate_templates(building)

    elif args.command == "validate":
        building = args.building if not args.all else None
        success = orchestrator.validate_templates(building)

    elif args.command == "full-pipeline":
        success = orchestrator.full_pipeline()

    else:
        parser.print_help()
        return 1

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
