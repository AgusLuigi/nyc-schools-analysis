#Export File

def save_dataframe_to_csv(df, folder_name="day_4_task", file_name="cleaned_sat_results.csv"):
    """
    Creates the target directory if it doesn't exist and saves the DataFrame as a CSV file.
    """
    # 1. Create the directory if it doesn't exist
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"✅ Directory '{folder_name}' has been created.")
    
    # 2. Define the full output path
    output_path = os.path.join(folder_name, file_name)
    
    # 3. Save the CSV file
    df.to_csv(output_path, index=False)
    
    print(f"✅ File successfully saved at: {output_path}")
    return output_path

# Aufruf der Funktion:
#save_dataframe_to_csv(df)