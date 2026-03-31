"""
Generate building folder structure.

Creates building directories and copies template for each property.
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional
import shutil

logger = logging.getLogger(__name__)


class BuildingInfo:
    """Represents a building/property."""

    def __init__(self, entity_code: str, building_name: str, address: Optional[str] = None):
        """
        Initialize building info.

        Args:
            entity_code: Entity code in Yardi (e.g., "204")
            building_name: Building name (e.g., "123 Main Street")
            address: Optional full address
        """
        self.entity_code = entity_code
        self.building_name = building_name
        self.address = address

    def folder_name(self) -> str:
        """
        Generate folder name.

        Returns:
            Formatted folder name "{entity_code} - {building_name}"
        """
        return f"{self.entity_code} - {self.building_name}"

    def __repr__(self) -> str:
        return f"BuildingInfo({self.entity_code}, {self.building_name})"


class FolderGenerator:
    """Generates building folder structures."""

    def __init__(self, template_path: Path, output_base_dir: Path):
        """
        Initialize generator.

        Args:
            template_path: Path to template.xlsx
            output_base_dir: Base directory where folders will be created
        """
        self.template_path = Path(template_path)
        self.output_base_dir = Path(output_base_dir)

        if not self.template_path.exists():
            raise FileNotFoundError(f"Template not found: {self.template_path}")

        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {
            "folders_created": 0,
            "templates_copied": 0,
            "errors": [],
        }

    def generate_for_buildings(self, buildings: List[BuildingInfo]) -> bool:
        """
        Generate folders for list of buildings.

        Args:
            buildings: List of BuildingInfo objects

        Returns:
            True if all successful
        """
        all_success = True

        for building in buildings:
            success = self.generate_for_building(building)
            if not success:
                all_success = False

        return all_success

    def generate_for_building(self, building: BuildingInfo) -> bool:
        """
        Generate folder structure for a single building.

        Creates:
            - {entity_code} - {building_name}/
              - {entity_code}_{building_name}_2027_Budget.xlsx
              - yardi_drops/

        Args:
            building: BuildingInfo object

        Returns:
            True if successful
        """
        try:
            # Create building folder
            folder_name = building.folder_name()
            building_path = self.output_base_dir / folder_name
            building_path.mkdir(parents=True, exist_ok=True)
            self.stats["folders_created"] += 1
            logger.info(f"Created folder: {building_path}")

            # Create yardi_drops subfolder
            yardi_drops_path = building_path / "yardi_drops"
            yardi_drops_path.mkdir(exist_ok=True)
            logger.info(f"Created yardi_drops: {yardi_drops_path}")

            # Copy template with building-specific name
            template_filename = self._generate_template_filename(building)
            template_copy_path = building_path / template_filename

            shutil.copy2(self.template_path, template_copy_path)
            self.stats["templates_copied"] += 1
            logger.info(f"Copied template: {template_copy_path}")

            return True

        except Exception as e:
            logger.error(f"Error generating folder for {building}: {e}")
            self.stats["errors"].append(f"{building}: {e}")
            return False

    @staticmethod
    def _generate_template_filename(building: BuildingInfo) -> str:
        """
        Generate template filename for building.

        Returns:
            Filename in format: {entity_code}_{building_name}_2027_Budget.xlsx
        """
        # Clean building name for filename (remove special characters)
        clean_name = building.building_name.replace(" ", "_")
        clean_name = "".join(c for c in clean_name if c.isalnum() or c == "_")

        return f"{building.entity_code}_{clean_name}_2027_Budget.xlsx"

    def get_stats(self) -> Dict:
        """
        Get generation statistics.

        Returns:
            Dict with stats on folders created and errors
        """
        return self.stats.copy()


class CSVBuildingLoader:
    """Loads building info from CSV file."""

    def __init__(self, csv_path: Path):
        """
        Initialize loader.

        Args:
            csv_path: Path to CSV file
        """
        self.csv_path = Path(csv_path)
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

    def load_buildings(self) -> List[BuildingInfo]:
        """
        Load buildings from CSV.

        Expected CSV columns: entity_code, building_name, address (optional)

        Returns:
            List of BuildingInfo objects
        """
        buildings = []

        try:
            with open(self.csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for row in reader:
                    entity_code = row.get("entity_code", "").strip()
                    building_name = row.get("building_name", "").strip()
                    address = row.get("address", "").strip() or None

                    if not entity_code or not building_name:
                        logger.warning(f"Skipping row with missing required fields: {row}")
                        continue

                    building = BuildingInfo(entity_code, building_name, address)
                    buildings.append(building)
                    logger.debug(f"Loaded building: {building}")

            logger.info(f"Loaded {len(buildings)} buildings from CSV")
            return buildings

        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise


def generate_folders_for_csv(template_path: Path, csv_path: Path,
                            output_base_dir: Path) -> bool:
    """
    Convenience function to generate folders from CSV.

    Args:
        template_path: Path to template.xlsx
        csv_path: Path to buildings.csv
        output_base_dir: Base output directory

    Returns:
        True if successful
    """
    loader = CSVBuildingLoader(csv_path)
    buildings = loader.load_buildings()

    generator = FolderGenerator(template_path, output_base_dir)
    success = generator.generate_for_buildings(buildings)

    stats = generator.get_stats()
    logger.info(f"Folder generation complete: {stats}")

    return success
