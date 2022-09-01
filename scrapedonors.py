from import_data import import_data
from get_latest_donations import get_latest_donations

# download the newest donations
get_latest_donations()

# process + import
import_data("/data/*.xlsx")

# build_site()
