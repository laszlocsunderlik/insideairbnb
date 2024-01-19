from decimal import Decimal
from datetime import date

import geojson
from passlib.context import CryptContext
from datetime import datetime, timedelta
from fastapi import Query, Depends, status, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from geojson import MultiPolygon
from shapely.wkt import loads
from shapely.geometry import mapping

from app.schemas import TokenData
from config import settings

SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def json_serializable(datas):
    # Convert Decimal objects to float for serialization
    for data in datas:
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, date):
                data[key] = value.isoformat()
    return datas


def get_pagination_params(
    # offset must be greater than or equal to 0
    offset: int = Query(0, ge=0),
    # limit must be greater than 0
    limit: int = Query(10, gt=0)
):
    return {"offset": offset, "limit": limit}


def hash_password(password: str):
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_access_token(token: str, credentials_exception: HTTPException):
    try:
        print(f"This is the token: {token}")
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        id: int = payload.get("user_id")
        print(id)
        if id is None:
            raise credentials_exception
        token_data = TokenData(id=id)
    except JWTError:
        raise credentials_exception
    return token_data


def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=f"Could not validate credentials", headers={"WWW-Authenticate": "Bearer"})
    return verify_access_token(token, credentials_exception)

# data="SRID=4326;MULTIPOLYGON(((4.991669 52.324436,4.991755 52.324289,4.991828 52.324175,4.991894 52.324077,4.991952 52.323996,4.992036 52.32387,4.992109 52.323767,4.99217 52.323706,4.992597 52.323135,4.993457 52.32195,4.994212 52.321029,4.99435 52.320829,4.994392 52.320774,4.994406 52.32076,4.994621 52.320511,4.994821 52.320267,4.994833 52.320241,4.994884 52.320184,4.995243 52.319674,4.99541 52.319446,4.995604 52.31921,4.995819 52.318968,4.996035 52.318706,4.99612 52.318579,4.996189 52.318501,4.996431 52.318201,4.996455 52.318169,4.996719 52.317849,4.996734 52.317824,4.996914 52.317652,4.996334 52.317348,4.995533 52.316932,4.995051 52.31669,4.994432 52.316389,4.994223 52.316289,4.993805 52.316093,4.993385 52.315898,4.992748 52.31561,4.99232 52.31542,4.991889 52.315234,4.991456 52.315049,4.991135 52.314914,4.990612 52.314698,4.990086 52.314486,4.989555 52.314278,4.989129 52.314114,4.988382 52.313833,4.988081 52.313724,4.987629 52.31356,4.986869 52.313294,4.98641 52.313138,4.986101 52.313036,4.984607 52.312522,4.978672 52.310487,4.971418 52.307995,4.967561 52.312216,4.965175 52.314817,4.963354 52.316826,4.959333 52.321219,4.956752 52.324045,4.957535 52.324319,4.961027 52.325519,4.962509 52.326036,4.964041 52.326555,4.967542 52.327758,4.968461 52.327221,4.968869 52.326984,4.96921 52.326783,4.969567 52.326547,4.969609 52.326518,4.970255 52.326169,4.970403 52.326252,4.970501 52.326302,4.970711 52.326406,4.970824 52.326465,4.97086 52.326486,4.970954 52.326544,4.971059 52.326616,4.971088 52.326639,4.971142 52.326686,4.971172 52.326716,4.97122 52.326771,4.971313 52.326887,4.971363 52.326958,4.971808 52.327473,4.972153 52.327716,4.973415 52.328474,4.974025 52.328907,4.974507 52.329327,4.974825 52.329701,4.975003 52.329977,4.975307 52.330562,4.975635 52.330607,4.975962 52.330636,4.976343 52.33065,4.977136 52.330642,4.977946 52.33116,4.978346 52.331077,4.978399 52.331074,4.978599 52.331065,4.979799 52.331003,4.98051 52.330783,4.980683 52.330761,4.980912 52.330762,4.981135 52.33077,4.98165 52.330805,4.982071 52.330803,4.98252 52.330804,4.983082 52.330768,4.983355 52.330747,4.983675 52.330709,4.983966 52.330667,4.984166 52.330625,4.984476 52.330564,4.984789 52.330481,4.984947 52.330428,4.985098 52.330363,4.985302 52.330259,4.985498 52.330144,4.986039 52.329811,4.986368 52.329599,4.986526 52.329486,4.986635 52.329413,4.987182 52.329,4.987287 52.328936,4.987372 52.328885,4.987648 52.328729,4.98875 52.328154,4.988916 52.328065,4.989054 52.327981,4.989224 52.327863,4.98942 52.327714,4.989555 52.327595,4.98965 52.327507,4.990477 52.326643,4.990717 52.326378,4.99079 52.326283,4.990857 52.326187,4.990911 52.326102,4.990961 52.326016,4.991007 52.325929,4.991055 52.325828,4.991165 52.325565,4.991436 52.324935,4.991614 52.324538,4.991669 52.324436)))"
#
# def geom_to_coordinates(input_data):
#     # Remove SRID=4326;MULTIPOLYGON(( and ))
#     input_data = input_data.split(";")
#     shapely_geom = loads(input_data[1])
#     geojson_dict = mapping(shapely_geom)
#     print(type(geojson_dict))
#     print(geojson_dict)
#     geojson_str = geojson.dumps(geojson_dict)
#     print(type(geojson_str))
#     print(geojson_str)
#
#
#     srid = input_data[0].split("=")[1]
#     print(srid)
#     return shapely_geom, srid
#
# geom_to_coordinates(input_data=data)