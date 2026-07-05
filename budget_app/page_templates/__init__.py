# Page templates package — extracted from workflow.py 2026-07-05
# (clean-architecture tranche 1). One module per page; this __init__
# re-exports so consumers keep flat `from page_templates import X`.
from .admin import ADMIN_TEMPLATE
from .dashboard import DASHBOARD_TEMPLATE
from .action_center import ACTION_CENTER_TEMPLATE
from .building_detail import BUILDING_DETAIL_TEMPLATE
from .pm_portal import PM_PORTAL_TEMPLATE
from .pm_edit import PM_EDIT_TEMPLATE
from .board_notice import BOARD_NOTICE_TEMPLATE

__all__ = [
    "ADMIN_TEMPLATE", "DASHBOARD_TEMPLATE", "ACTION_CENTER_TEMPLATE",
    "BUILDING_DETAIL_TEMPLATE", "PM_PORTAL_TEMPLATE", "PM_EDIT_TEMPLATE",
    "BOARD_NOTICE_TEMPLATE",
]
