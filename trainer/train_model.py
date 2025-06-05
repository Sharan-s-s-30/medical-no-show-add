import os
import joblib
import pandas as pd
from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine

def main():
    # Load env & connect
    load_dotenv()
    user = os.getenv("POSTGRES_USER", "ml_user")
    pwd  = os.getenv("POSTGRES_PASS", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "ml_db")
    uri  = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(uri)

    # SELECT the processed table
    df = pd.read_sql("SELECT * FROM processed_appointments", engine)
    print(f"Loaded {len(df)} rows from processed_appointments")

    # Split into train / temp (train 70%, temp 30%)
    train_df, temp_df = train_test_split(df, train_size=0.7, random_state=42, stratify=df["no_show"])
    # Split further into validation & test
    val_df, test_df  = train_test_split(temp_df, train_size=2/3, random_state=42, stratify=temp_df["no_show"])

    # Push test_df into Postgres for later use
    print(f"Writing {len(test_df)} test rows back to Postgres…")
    test_df.to_sql(
        "test_appointments",
        con=engine,
        if_exists="replace",
        index=False,
    )
    print("test_appointments table updated.")

    print(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")

    # Features & target
    target = "no_show"
    # numeric cols
    num_feats = ["age", "wait_days", "scheduled_hour", "appointment_weekday"]
    # categorical
    cat_feats = ["gender", "neighbourhood", "age_group"]

    X_train = train_df[num_feats + cat_feats]
    y_train = train_df[target]

    # Build pipeline
    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), num_feats),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_feats),
    ], remainder="drop")

    clf = Pipeline([
        ("pre", preprocessor),
        ("lr", LogisticRegression(max_iter=1000)),
    ])

    # Fit
    print("Training model…")
    clf.fit(X_train, y_train)

    # Save
    os.makedirs("models", exist_ok=True)
    path = os.path.join("models", "no_show_model.joblib")
    joblib.dump(clf, path)
    print(f"Model saved to {path}")

if __name__ == "__main__":
    main()
