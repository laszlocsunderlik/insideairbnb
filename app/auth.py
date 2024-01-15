from fastapi import APIRouter, HTTPException, status
from app.schemas import UserLogin, UserCreate, UserOut
from passlib.context import CryptContext


router = APIRouter(tags=["auth"])
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
#
#
# def hash_password(password: str):
#     return pwd_context.hash(password)
#
#
# def verify_password(plain_password: str, hashed_password: str):
#     return pwd_context.verify(plain_password, hashed_password)
#
#
# @router.post("/create_user", status_code=status.HTTP_201_CREATED, response_model=UserOut)
# async def create_user(user: UserCreate, cursor):
#     # hashed_password = hash_password(user.password)
#     new_user = cursor.execute("INSERT INTO users (username, password) VALUES (%s, %s);",
#                               (user.username, user.password))
#     new_user.fetchfirst()  # fetchfirst() returns the first row of the result set
#     return {"message": "User created successfully",
#             "user": new_user}
#
#
# @router.post("/login")
# async def login(user_credentials: UserLogin, cursor):
#     cursor.execute("SELECT * FROM users where username = %s;", (user_credentials.username,))
#     user = cursor.fetchfirst()
#
#     if not user:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
#                             detail=f"Invalid credentials")
#
#     if not verify_password(user_credentials.password, user.password):
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
#                             detail=f"Invalid credentials")
#
#     # create token
#     # return token
#
#     return {"message": "Login successful",
#             "user": user.username}
