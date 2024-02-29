import json
from datetime import datetime
from typing import Set, Dict, List

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, field_validator
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker, declarative_base

from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# SQLAlchemy model
class ProcessedAgentDataInDB(Base):
    __tablename__ = "processed_agent_data"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    road_state = Column(String)
    user_id = Column(Integer)
    x = Column(Float)
    y = Column(Float)
    z = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    timestamp = Column(DateTime)


Base.metadata.create_all(engine)


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


class ProcessedAgentDataWithId(ProcessedAgentData):
    id: int


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# Mapper functions
def db_model_to_response_model(db_model: ProcessedAgentDataInDB) -> ProcessedAgentDataWithId:
    return ProcessedAgentDataWithId(
        id=db_model.id,
        road_state=db_model.road_state,
        agent_data=AgentData(
            user_id=db_model.user_id,
            accelerometer=AccelerometerData(x=db_model.x, y=db_model.y, z=db_model.z),
            gps=GpsData(latitude=db_model.latitude, longitude=db_model.longitude),
            timestamp=db_model.timestamp,
        )
    )


def db_models_to_response_models(db_models: List[ProcessedAgentDataInDB]) -> List[ProcessedAgentDataWithId]:
    return [db_model_to_response_model(db_model) for db_model in db_models]


# FastAPI CRUDL endpoints

@app.post("/processed_agent_data/", response_model=List[ProcessedAgentDataWithId])
def create_processed_agent_data(data: List[ProcessedAgentData]):
    db_objects = [
        ProcessedAgentDataInDB(
            road_state=item.road_state,
            user_id=item.agent_data.user_id,
            x=item.agent_data.accelerometer.x,
            y=item.agent_data.accelerometer.y,
            z=item.agent_data.accelerometer.z,
            latitude=item.agent_data.gps.latitude,
            longitude=item.agent_data.gps.longitude,
            timestamp=item.agent_data.timestamp,
        )
        for item in data
    ]

    with SessionLocal() as session:
        session.add_all(db_objects)
        session.commit()

    return db_models_to_response_models(db_objects)


@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataWithId)
def read_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as session:
        result = session.query(ProcessedAgentDataInDB).filter(
            ProcessedAgentDataInDB.id == processed_agent_data_id).first()
        if result is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return db_model_to_response_model(result)


@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataWithId])
def list_processed_agent_data():
    with SessionLocal() as session:
        db_objects = session.query(ProcessedAgentDataInDB).all()
        return db_models_to_response_models(db_objects)


@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataWithId)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    with SessionLocal() as session:
        db_data = session.query(ProcessedAgentDataInDB).filter(
            ProcessedAgentDataInDB.id == processed_agent_data_id).first()
        if db_data is None:
            raise HTTPException(status_code=404, detail="Item not found")

        for key, value in data.dict().items():
            setattr(db_data, key, value)

        session.add(db_data)
        session.commit()
        session.refresh(db_data)
        return db_model_to_response_model(db_data)


@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataWithId)
def delete_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as session:
        db_data = session.query(ProcessedAgentDataInDB).filter(
            ProcessedAgentDataInDB.id == processed_agent_data_id).first()
        if db_data is None:
            raise HTTPException(status_code=404, detail="Item not found")

        session.delete(db_data)
        session.commit()
        return db_model_to_response_model(db_data)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
