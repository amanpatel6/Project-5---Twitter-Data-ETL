import pandas as pd
import numpy as np

# EXTRACT
def extract(file_path):
    df = pd.read_csv(file_path)

    return df

# TRANSFORM

# Cleaning username column
def transform(df):
    df["username"] = df["username"].astype(str).str.strip()

    df["username"] = np.where(                  #this is essentially a IF ELSE statement. so where username starts with @, keep username ELSE add @ before it
        df["username"].str.startswith("@"),
        df["username"],
        "@" + df["username"]
    )

    df = df[~df["username"].str.contains("bot")].copy() # ~ is a logical NOT operator in python - in this case, will keep all rows that does NOT contain "bot". Have to make a copy as this 

    # Cleaning location column
    df["location"] = df["location"].astype(str).str.strip().replace("", None).fillna("Unknown")

    # Dropping text column
    df = df.drop(columns=["text"])

    # Cleaning likes column
    df["likes"] = df["likes"].fillna(0).astype(int) # Converets all NA values to 0

    # Cleaning retweets column
    df["retweets"] = df["retweets"].fillna(0).astype(int)

    # Cleaning hashtags column
    df["hashtags"] = df["hashtags"].astype(str).str.strip().replace("", None).fillna("Unknown")  #put the below 3 lines into one
    # df["hashtags"] = df["hashtags"].astype(str).str.strip()
    # df["hashtags"] = df["hashtags"].replace("", None)   
    # df["hashtags"] = df["hashtags"].fillna(None)


    # Cleaning verified column
    df["verified"] = df["verified"].astype(str).str.lower().fillna("Unknown")

    yes_no_mapping = {
        "true": "Yes",
        "false": "No",
        "yes": "Yes",
        "no": "No",
        "unknown": "Unknown"
    }

    df["verified"] = df["verified"].map(yes_no_mapping)

    # Cleaning created_at column
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")
    df["created_at"] = df["created_at"].fillna("Unknown")


    return df

# LOAD
def load(df):
    df.to_csv("Cleaned Twitter Dataset.csv", index=False)
    
    return df

# FUNCTIONS
def main():
    file_path = "/Users/amanpatel/Desktop/Data Engineering Projects/Twitter Data ETL Project (5)/twitter_unique_usernames_messy_date.csv"
    df = extract(file_path)
    df = transform(df)
    load(df)

if __name__ == "__main__":
    main()