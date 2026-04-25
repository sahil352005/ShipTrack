from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from auth.service import verify_password, create_access_token, get_current_user, get_password_hash
from pydantic import BaseModel, EmailStr

router = APIRouter(prefix="/auth", tags=["Authentication"])

class UserRegister(BaseModel):
    full_name: str
    email: EmailStr
    password: str

@router.post("/register")
async def register(user_in: UserRegister):
    from main import database
    # Simple register (defaults to Analyst)
    check_query = "SELECT * FROM users WHERE email = :email"
    existing = await database.fetch_one(check_query, {"email": user_in.email})
    if existing:
        raise HTTPException(status_code=400, detail="User already exists")
    
    query = "INSERT INTO users (full_name, email, password_hash, role) VALUES (:name, :email, :pass, 'Analyst')"
    await database.execute(query, {"name": user_in.full_name, "email": user_in.email, "pass": get_password_hash(user_in.password)})
    return {"message": "User registered successfully"}

@router.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    from main import database
    query = "SELECT * FROM users WHERE email = :email"
    user = await database.fetch_one(query, {"email": form_data.username})
    
    if not user or not verify_password(form_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(
        data={"sub": user["email"], "role": user["role"], "name": user["full_name"]}
    )
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me")
async def read_users_me(current_user: dict = Depends(get_current_user)):
    return current_user
