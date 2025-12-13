"""
Get Additional Stats Module

Processes additional stats from match facts (goals, cards, assists, injuries).
Transforms events into a player-centric dictionary for merging with player stats.
"""

import json


def process_additional_stats(match_facts):
    """
    Process additional stats from match facts, transforming events into a player-centric dictionary.
    
    Args:
        match_facts: Dictionary containing match facts from API response
        
    Returns:
        Dictionary mapping player_id to list of additional stat dictionaries
        Example: {
            "12345": [{"yellow_cards": 1}, {"scores": [...]}],
            "67890": [{"assisted_player_id": "12345"}]
        }
    """
    if match_facts is None:
        return {}
        
    events = match_facts.get("events", {}).get("events", [])

    processed_events = {}
    event_types = ["Card", "Goal", "Assist", "Yellow", "Injuries"] 

    for event in events:
        player_id = event.get("playerId")
        event_type = event.get("type")
        
        # Skip events without a player ID or not in our target list
        if not player_id or event_type not in event_types:
            continue
            
        event_details = {}

        if event_type == "Goal":
            shot_data = event.get("shotmapEvent", {})
            if player_id not in processed_events:
                processed_events[player_id] = []

            # Find existing goal record for this player
            goal_event = next((e for e in processed_events[player_id] if "scores" in e), None)
            if goal_event:
                goal_event["scores"].append(shot_data)
            else:
                event_details["scores"] = [shot_data]
                processed_events[player_id].append(event_details)

        elif event_type == "Assist":
            event_details["assisted_player_id"] = event.get("assistedPlayerId")

        elif event_type in ("Card", "Yellow"):
            card_type = event.get("card")

            if card_type == "Yellow" or card_type == "YellowRed":
                if player_id not in processed_events:
                    processed_events[player_id] = []

                # Find existing yellow card record for this player
                yellow_card_event = next((e for e in processed_events[player_id] if "yellow_cards" in e), None)

                if yellow_card_event:
                    yellow_card_event["yellow_cards"] += 1
                    if card_type == "YellowRed":
                        yellow_card_event["yellow_red"] = True  # Mark yellow-red event
                else:
                    event_details["yellow_cards"] = 1
                    if card_type == "YellowRed":
                        event_details["yellow_red"] = True
                    processed_events[player_id].append(event_details)
        
        # Add non-yellow-card events normally
        if player_id not in processed_events or not any("yellow_cards" in e for e in processed_events[player_id]):
            processed_events.setdefault(player_id, []).append(event_details)

    return processed_events