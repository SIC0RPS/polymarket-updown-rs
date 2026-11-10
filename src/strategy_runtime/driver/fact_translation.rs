use crate::observability::action::ActionEvent;
use crate::strategy::StrategyFact;

pub fn translate_strategy_fact(fact: StrategyFact) -> Option<ActionEvent> {
    match fact {
        StrategyFact::CtfOwnedLegOutcome(_) => None,
    }
}

pub fn translate_strategy_facts(facts: Vec<StrategyFact>) -> Vec<ActionEvent> {
    facts
        .into_iter()
        .filter_map(translate_strategy_fact)
        .collect()
}
