from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from auth.service import get_current_user, check_role, get_password_hash

router = APIRouter(prefix="/users", tags=["Users"])

class UserCreate(BaseModel):
    full_name: str
    email: EmailStr
    password: str
    role: str = "Analyst"

class UserOut(BaseModel):
    id: int
    full_name: str
    email: str
    role: str
    is_active: bool

@router.post("/create", response_model=UserOut)
async def create_user(user_in: UserCreate, admin: dict = Depends(check_role(["Admin"]))):
    from main import database
    
    # Check if user exists
    check_query = "SELECT * FROM users WHERE email = :email"
    existing = await database.fetch_one(check_query, {"email": user_in.email})
    if existing:
        raise HTTPException(status_code=400, detail="User with this email already exists")
    
    hashed_password = get_password_hash(user_in.password)
    query = """
        INSERT INTO users (full_name, email, password_hash, role)
        VALUES (:full_name, :email, :password, :role)
        RETURNING id, full_name, email, role, is_active
    """
    values = {
        "full_name": user_in.full_name,
        "email": user_in.email,
        "password": hashed_password,
        "role": user_in.role
    }
    
    new_user = await database.fetch_one(query, values)
    return dict(new_user)

@router.get("/", response_model=List[UserOut])
async def list_users(admin: dict = Depends(check_role(["Admin"]))):
    from main import database
    query = "SELECT id, full_name, email, role, is_active FROM users"
    rows = await database.fetch_all(query)
    return [dict(r) for r in rows]
