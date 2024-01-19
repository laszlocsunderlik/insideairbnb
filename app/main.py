from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from psycopg2.extras import RealDictCursor
from geojson import Feature, FeatureCollection, Point
from app.db import database
from app.schemas import *
from app.utils import *

app = FastAPI()


@app.get("/")
async def root():
    return {"Message": "Hi insideairbnb app"}


@app.post("/create_user/", status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate, cursor: RealDictCursor = Depends(database.connect)):
    try:
        hashed_password = hash_password(user.password)
        cursor.execute("INSERT INTO \"user\" (username, password) VALUES (%s, %s) RETURNING id, username;",
                       (user.username, hashed_password))

        # Fetch the newly created user information
        new_user = cursor.fetchone()
        # Commit the changes to the database
        cursor.connection.commit()
        return {"message": "User created successfully",
                "user": new_user}
    except Exception as e:
        # Handle exceptions, log them, or customize the error response as needed
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/login/", response_model=Token)
async def login(user_credentials: OAuth2PasswordRequestForm = Depends(), cursor=Depends(database.connect)):
    cursor.execute("SELECT * FROM \"user\" where username = %s;", (user_credentials.username,))
    user = cursor.fetchone()
    print(user)
    if not user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Invalid credentials1")

    if not verify_password(user_credentials.password, user["password"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Invalid credentials2")

    access_token = create_access_token(data={"user_id": user["id"]})

    return {"message": "Login successful",
            "user": user["username"],
            "access_token": access_token,
            "token_type": "bearer"}


@app.get("/listings/")
async def get_listings(query_date: str = Query(..., description="Start date to query from (inclusive)"),
                       pagination: dict = Depends(get_pagination_params),
                       cursor: RealDictCursor = Depends(database.connect),
                       user_id: int = Depends(get_current_user)):

    # Get the offset and limit values from the pagination dictionary
    offset = pagination["offset"]
    limit = pagination["limit"]
    end = offset + limit

    cursor.execute("SELECT * FROM listings where download_date = %s;", (query_date, ))
    listings = cursor.fetchall()[offset:end]
    print(len(listings))

    if not listings:
        raise HTTPException(status_code=404, detail=f"Listings with {query_date} not found")  # Use 404 directly

    listings_jsonable = [jsonable_encoder(Listings(**listing)) for listing in listings]

    features = []
    for listing in listings_jsonable:
        feature = Feature(
            geometry=Point((listing["latitude"], listing["longitude"])),
            properties=jsonable_encoder(Listings(**listing))
        )
        features.append(feature)

    feature_collection = FeatureCollection(features)

    response = {
        "limit": limit,
        "offset": offset,
        "end": end,
        "total": len(listings),
        "results": feature_collection
    }

    return JSONResponse(content=response)


@app.get("/neighbourhoods/")
async def get_neighbourhoods(query_date: str = Query(..., description="Start date to query from (inclusive)"),
                             pagination: dict = Depends(get_pagination_params),
                             cursor: RealDictCursor = Depends(database.connect),
                             user_id: int = Depends(get_current_user)):
    # Get the offset and limit values from the pagination dictionary
    offset = pagination["offset"]
    limit = pagination["limit"]

    # Calculate the end index for slicing the items list
    end = offset + limit

    cursor.execute("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(features.feature)
            )
            FROM (
                SELECT json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geometry)::jsonb,
                    'properties', to_jsonb(inputs) - 'geometry'
            ) AS feature
            FROM (SELECT * FROM neighbourhoods where download_date =%s) inputs) features;
        """, (str(query_date), ))

    neighbourhoods = cursor.fetchall()[offset:end]

    if not neighbourhoods:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"With {query_date}: neighbourhoods was not found")
    response = {
        "limit": limit,
        "offset": offset,
        "end": end,
        "total": len(neighbourhoods),
        "results": neighbourhoods[0]["json_build_object"]
    }
    return JSONResponse(content=response)

