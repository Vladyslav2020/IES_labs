from app.entities.agent_data import AgentData, AccelerometerData
from app.entities.processed_agent_data import ProcessedAgentData


def classify_road_state(accelerometer_data: AccelerometerData) -> str:
    z_axis_value = accelerometer_data.z

    if z_axis_value < 10000:
        return "bumpy"
    elif z_axis_value > 16600:
        return "hilly"
    else:
        return "normal"


def process_agent_data(
        agent_data: AgentData,
) -> ProcessedAgentData:
    """
    Process agent data and classify the state of the road surface.
    Parameters:
        agent_data (AgentData): Agent data that containing accelerometer, GPS, and timestamp.
    Returns:
        processed_data_batch (ProcessedAgentData): Processed data containing the classified state of the road surface and agent data.
    """
    road_state = classify_road_state(agent_data.accelerometer)
    processed_data = ProcessedAgentData(road_state=road_state, agent_data=agent_data)
    return processed_data
