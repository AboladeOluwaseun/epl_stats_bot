import re

def get_form_visualizer(form_str: str) -> str:
    """Convert WDL string into colored emoji indicators."""
    if not form_str:
        return "N/A"
    
    # Map letters to emojis
    mapping = {
        'W': '🟢', 
        'D': '🟡', 
        'L': '🔴'
    }
    
    visual = ""
    # Process up to last 5 matches
    for char in form_str[-5:]:
        visual += mapping.get(char.upper(), '⚪')
    
    return visual

def get_rating_bar(rating: float) -> str:
    """Create a visual bar for ratings (0-10)."""
    if not rating:
        return "☆☆☆☆☆"
    
    # Scale 10 to 5 units
    filled = round(rating / 2.0)
    bar = "★" * filled + "☆" * (5 - filled)
    return bar

def format_player_stats(player: dict, stats: dict = None) -> str:
    """Format player profile and statistics for WhatsApp with premium design."""
    header = f"👤 *PLAYER PROFILE*"
    divider = "━━━━━━━━━━━━━━━━━━"
    
    bio = (
        f"{header}\n"
        f"{divider}\n"
        f"⭐ *{player['name'].upper()}*\n"
        f"🏳️ *Nationality:* {player['nationality'] or 'N/A'}\n"
        f"🏃 *Position:* {player['position'] or 'N/A'}\n"
        f"🔢 *Number:* #{player['number'] or '?'}\n"
        f"🎂 *Age:* {player['age'] or 'N/A'}yrs  |  📏 *{player['height'] or 'N/A'}*"
    )

    if stats:
        rating_val = float(stats['rating']) if stats['rating'] else 0.0
        rating_bar = get_rating_bar(rating_val)
        
        performance = (
            f"\n\n📊 *LATEST PERFORMANCE*\n"
            f"{divider}\n"
            f"📅 *{stats['date']}*\n"
            f"⚔️ *Match:* {stats['matchup']}\n"
            f"👕 *Team:* {stats['team']}\n\n"
            f"⏱️ *Minutes:* {stats['minutes'] or 0}'\n"
            f"🎯 *Shots (On Target):* {stats['shots_on_target'] or 0}\n"
            f"🥅 *Goals:* {stats['goals'] or 0}  |  🅰️ *Assists:* {stats['assists'] or 0}\n"
            f"📈 *Pass Accuracy:* {stats['passes_acc'] or 0}%\n"
            f"🌟 *Rating:* {rating_val:.1f} {rating_bar}"
        )
        return bio + performance
    else:
        return bio + f"\n\n⚠️ _No recent match stats found._"

def format_team_results(team_name: str, results: list) -> str:
    """Format team results with form visualizers."""
    if not results:
        return f"🤷 No recent results found for *{team_name}*."

    header = f"📊 *LATEST RESULTS: {team_name.upper()}*"
    divider = "━━━━━━━━━━━━━━━━━━"
    
    # Calculate mini-form from results
    form_chars = ""
    for r in results:
        if r['home_team'].lower() in team_name.lower():
            if r['home_goals'] > r['away_goals']: form_chars += 'W'
            elif r['home_goals'] < r['away_goals']: form_chars += 'L'
            else: form_chars += 'D'
        else:
            if r['away_goals'] > r['home_goals']: form_chars += 'W'
            elif r['away_goals'] < r['home_goals']: form_chars += 'L'
            else: form_chars += 'D'
            
    form_visual = get_form_visualizer(form_chars[::-1]) # results are desc, so reverse for chronological
    
    msg = f"{header}\n{divider}\n"
    msg += f"📈 *Recent Form:* {form_visual}\n\n"
    
    for r in results:
        date_short = r['date']
        home = r['home_team']
        away = r['away_team']
        score = f"*{r['home_goals']} - {r['away_goals']}*"
        
        msg += f"📅 {date_short}\n"
        msg += f"⚔️ {home} {score} {away}\n\n"
        
    return msg

def format_head_to_head(team1: str, team2: str, matches: list) -> str:
    """Format head-to-head match results with analytic summary."""
    if not matches:
        return f"🤷 No recent matches found between *{team1}* and *{team2}*."

    # Analytics
    t1_wins = 0
    t2_wins = 0
    draws = 0
    
    for m in matches:
        if m['home_goals'] > m['away_goals']:
            if team1.lower() in m['home_team'].lower(): t1_wins += 1
            else: t2_wins += 1
        elif m['home_goals'] < m['away_goals']:
            if team1.lower() in m['away_team'].lower(): t1_wins += 1
            else: t2_wins += 1
        else:
            draws += 1

    header = f"🆚 *H2H: {team1.upper()} vs {team2.upper()}*"
    divider = "━━━━━━━━━━━━━━━━━━"
    
    msg = (
        f"{header}\n"
        f"{divider}\n"
        f"📊 *Summary (Last {len(matches)}):*\n"
        f"✅ {team1}: {t1_wins}W  |  🤝 Draws: {draws}  |  ✅ {team2}: {t2_wins}W\n\n"
    )
    
    latest = matches[0]
    msg += f"🏁 *LATEST:* {latest['date']}\n"
    msg += f"🏟️ {latest['venue']}\n"
    msg += f"👉 {latest['home_team']} *{latest['home_goals']}-{latest['away_goals']}* {latest['away_team']}\n\n"
    
    if len(matches) > 1:
        msg += "📜 *PAST ENCOUNTERS:*\n"
        for m in matches[1:]:
            msg += f"• {m['date']}: {m['home_team']} {m['home_goals']}-{m['away_goals']} {m['away_team']}\n"

    return msg

def format_standings(standings: list) -> str:
    """Format league standings with qualification markers."""
    if not standings:
        return "📉 No standings data available."

    header = "🏆 *PREMIER LEAGUE TABLE*"
    divider = "━━━━━━━━━━━━━━━━━━"
    
    msg = (
        f"{header}\n"
        f"{divider}\n"
        f"`#   TEAM        P   GD   PTS`\n"
    )
    
    for row in standings:
        # Markers
        prefix = " "
        if row['rank'] <= 4: prefix = "⭐" # UCL
        elif row['rank'] <= 6: prefix = "🔷" # UEL
        elif row['rank'] >= 18: prefix = "🔻" # Relegation
        
        name = row['team'][:10]
        # Using fixed widths in monospaced block
        line = f"{str(row['rank']).ljust(2)} {name.ljust(11)} {str(row['played']).ljust(2)} {str(row['gd']).rjust(3)} {str(row['points']).rjust(3)}"
        msg += f"{prefix} `{line}`\n"
    
    msg += f"{divider}\n"
    msg += "_⭐ UCL | 🔷 UEL | 🔻 Drop_"
    return msg
