# Generalized CSV Deduplication Tool
import os
import pandas as pd

# Load all CSVs from folder and prompt user for mapping
import os
csv_folder = 'uploads/'  # Folder with uploaded CSVs

# Manually define the CSV filenames in order to match with already merged data csv
csv_filenames_ordered = [
    "Liv Labs Master Mailing List.csv",
    "Melody's Cabal Members.csv",
    "Melody's LinkedIn Connections.csv",
    "Melody's Personal Contacts.csv",
    "Liv Labs Pelvic Health Influencer List.csv",
    "Waitlist Signup List.csv"
]

# Create full paths based on that order
csv_files = [os.path.join(csv_folder, f) for f in csv_filenames_ordered]
csv_configs = []


# Prompt user to configure each CSV
for filename in csv_files:
    print(f"\nPreviewing: {filename}")
    header_row = input(f"Which row contains headers in '{filename}'? (0-based index, default is 0): ").strip()
    header_row = int(header_row) if header_row.isdigit() else 0

    df = pd.read_csv(filename, header=header_row)
    df.columns = df.columns.str.strip()  # Normalize column names , ran into issues with .lower() due to some columns having identical names
    print("First few rows:\n", df.head())
    print("Available columns:", list(df.columns))

    # Prompt user for mappings
    email_col = input(f"\nColumn name for EMAIL in '{filename}': ").strip() or None
    if not email_col:
        print(f"EMAIL column is required for '{filename}'. Skipping this file.")
        continue  

    name_full_col = input("Column for FULL NAME (leave blank if using separate names): ").strip()

    if not name_full_col:
        name_full_col = None
        first_name_col = input("Column for FIRST NAME: ").strip() or None
        last_name_col = input("Column for LAST NAME: ").strip() or None
    else:
        first_name_col = None
        last_name_col = None

    linkedin_col = input("Column for LinkedIn URL (leave blank if none): ").strip() or None

    config = {
        'path': filename,
        'prefix': filename.replace('.csv', '_').replace(" ", "_").lower(),
        'mappings': {
            'email': email_col,
            'first_name': first_name_col,
            'last_name': last_name_col,
            'linkedin': linkedin_col,
            'name_full': name_full_col
        },
        'source': filename.replace('.csv', ''),
        'header_row': header_row
    }
    csv_configs.append(config)


# Preprocessing function
def preprocess_csv(config):
    df = pd.read_csv(config['path'], header=config['header_row'])
    df.columns = df.columns.str.strip()  # Normalize column names
    df_prefixed = df.add_prefix(config['prefix'])

    # Check that required columns exist
    for key in ['email', 'first_name', 'last_name', 'linkedin', 'name_full']:
        col = config['mappings'].get(key)
        if col and col not in df.columns:
            raise ValueError(f"Column '{col}' not found in {config['path']}. Available columns: {df.columns.tolist()}")

    email_std = df[config['mappings']['email']].str.strip() if config['mappings']['email'] else ''
    linkedin_std = df[config['mappings']['linkedin']].fillna("").astype(str).str.strip() if config['mappings']['linkedin'] else ''

    if config['mappings']['name_full']:
        name_split = df[config['mappings']['name_full']].str.strip().str.split()
        first_name_std = name_split.str[0]
        last_name_std = name_split.str[-1]
    else:
        first_name_std = df[config['mappings']['first_name']].str.strip() if config['mappings']['first_name'] else ''
        last_name_std = df[config['mappings']['last_name']].str.strip() if config['mappings']['last_name'] else ''

    df_prefixed['email_std'] = email_std
    df_prefixed['first_name_std'] = first_name_std
    df_prefixed['last_name_std'] = last_name_std
    df_prefixed['linkedin_url_std'] = linkedin_std
    df_prefixed['source'] = config['source']

    return df_prefixed

# Combine all processed dataframes
combined_df = pd.concat([preprocess_csv(cfg) for cfg in csv_configs], ignore_index=True)

# Matching function
def create_matched_dataframe(records_df):
    matched_groups = []
    processed_indices = set()

    for i, row1 in records_df.iterrows():
        if i in processed_indices:
            continue

        current_group = [i]
        match_status = 'UNMATCHED'

        for j, row2 in records_df.iterrows():
            if i != j and j not in processed_indices:
                if (row1['email_std'] and row1['email_std'] == row2['email_std']):
                    current_group.append(j)
                    match_status = 'MATCHED_ON_EMAIL'
                elif (row1['linkedin_url_std'] and 
                      row1['linkedin_url_std'].replace("www.", "") == row2['linkedin_url_std'].replace("www.", "")):
                    current_group.append(j)
                    if match_status != 'MATCHED_ON_EMAIL':
                        match_status = 'MATCHED_ON_LINKEDIN'
                elif (row1['first_name_std'] and row1['last_name_std'] and 
                      row1['first_name_std'] == row2['first_name_std'] and 
                      row1['last_name_std'] == row2['last_name_std']):
                    current_group.append(j)
                    if match_status not in ['MATCHED_ON_EMAIL', 'MATCHED_ON_LINKEDIN']:
                        match_status = 'MATCHED_ON_FIRST_NAME_LAST_NAME'

        for idx in current_group:
            processed_indices.add(idx)

        matched_groups.append({
            'indices': current_group,
            'match_status': match_status
        })

    return matched_groups

# Merge matched groups into final output
matched_groups = create_matched_dataframe(combined_df)
final_records = []

for group in matched_groups:
    merged_record = {'MATCH_STATUS': group['match_status']}

    for col in combined_df.columns:
        if col not in ['email_std', 'first_name_std', 'last_name_std', 'linkedin_url_std', 'source']:
            merged_record[col] = None

    for idx in group['indices']:
        record = combined_df.iloc[idx]
        for col, val in record.items():
            if col not in ['email_std', 'first_name_std', 'last_name_std', 'linkedin_url_std', 'source']:
                if pd.isna(merged_record[col]) and pd.notna(val):
                    merged_record[col] = val

    final_records.append(merged_record)

final_df = pd.DataFrame(final_records)
final_df.to_csv('deduplicated_output.csv', index=False)
print("Deduplicated output saved as 'deduplicated_output.csv'")