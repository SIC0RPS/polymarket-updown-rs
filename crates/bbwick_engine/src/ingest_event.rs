use crate::ids::{IdHeartbeatFrame, IdStepFrame, IdTickFrame};
use crate::wire::{HeartbeatFrame, PolymarketLastTradeFrame, StepFrame, TickFrame};

#[derive(Debug, Clone)]
pub enum IngestEvent {
    TickById(IdTickFrame),
    Tick(TickFrame),
    HeartbeatById(IdHeartbeatFrame),
    Heartbeat(HeartbeatFrame),
    StepById(IdStepFrame),
    Step(StepFrame),
    PolymarketLastTrade(PolymarketLastTradeFrame),
}
