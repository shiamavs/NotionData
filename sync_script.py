from notion_client import Client

# Initialize the Notion client
notion = Client(auth=os.getenv("NOTION_API_KEY"))

# Database IDs
LARGER_DATABASE_ID = "132f005e7e9b80769163e36a6a083e01"
SMALLER_DATABASE_IDS = {
    "Keshav Ji": "140f005e7e9b810ba4c7d0c6c3db7326",
    "Shiama Ji": "137f005e7e9b81bca296d9d70d25ad48",
}

# Fetch all items from a database
def fetch_database_items(database_id):
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        response = notion.databases.query(
            database_id=database_id,
            start_cursor=start_cursor
        )
        results.extend(response["results"])
        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor", None)

    return results

# Check if an entry exists in a smaller database by Sync ID
def find_item_by_sync_id(database_id, sync_id):
    response = notion.databases.query(
        database_id=database_id,
        filter={"property": "Sync ID", "rich_text": {"equals": sync_id}}
    )
    return response["results"][0] if response["results"] else None

# Create or update an item in a database
def upsert_item(database_id, item_data):
    sync_id = item_data["properties"]["Sync ID"]["rich_text"][0]["text"]["content"]
    existing_item = find_item_by_sync_id(database_id, sync_id)

    if existing_item:
        # Update existing item
        notion.pages.update(
            page_id=existing_item["id"],
            properties=item_data["properties"]
        )
    else:
        # Create new item
        notion.pages.create(parent={"database_id": database_id}, properties=item_data["properties"])

# Main routing logic
def sync_from_larger_to_smaller():
    larger_items = fetch_database_items(LARGER_DATABASE_ID)

    for item in larger_items:
        # Extract the "Category" field to determine routing
        category = item["properties"].get("Category", {}).get("select", {}).get("name")
        if category and category in SMALLER_DATABASE_IDS:
            smaller_db_id = SMALLER_DATABASE_IDS[category]

            # Prepare item data
            item_data = {
                "properties": {
                    "Name": item["properties"]["Name"],
                    "Sync ID": item["properties"]["Sync ID"],
                    # Add other fields here as needed
                }
            }

            # Sync item to the smaller database
            upsert_item(smaller_db_id, item_data)

def sync_from_smaller_to_larger():
    for category, smaller_db_id in SMALLER_DATABASE_IDS.items():
        smaller_items = fetch_database_items(smaller_db_id)

        for item in smaller_items:
            sync_id = item["properties"]["Sync ID"]["rich_text"][0]["text"]["content"]
            larger_item = find_item_by_sync_id(LARGER_DATABASE_ID, sync_id)

            # Prepare item data
            item_data = {
                "properties": {
                    "Name": item["properties"]["Name"],
                    "Sync ID": item["properties"]["Sync ID"],
                    "Category": {"select": {"name": category}},
                    # Add other fields here as needed
                }
            }

            # Sync item to the larger database
            upsert_item(LARGER_DATABASE_ID, item_data)

# Run the sync processes
if __name__ == "__main__":
    print("Syncing from larger to smaller databases...")
    sync_from_larger_to_smaller()
    print("Syncing from smaller to larger database...")
    sync_from_smaller_to_larger()
    print("Sync complete.")
