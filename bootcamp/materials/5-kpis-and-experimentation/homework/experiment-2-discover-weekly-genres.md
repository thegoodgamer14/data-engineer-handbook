# Experiment 2: Genre Expansion in Discover Weekly

## Hypothesis
**"Introducing unrelated genres alongside related genres in Discover Weekly will increase user musical exploration and long-term engagement."**

## Success Metrics
**Primary KPIs Impacted:**
- User Engagement Score (expanded musical horizons)
- Discovery Success Rate (especially for unrelated genres)
- Time to First Save (from Discover Weekly tracks)

**Secondary Metrics:**
- Genre diversity in user's saved music
- Cross-genre playlist creation
- Long-term retention rates (8-week follow-up)
- Discover Weekly completion rate

## Test Design

### Control vs Treatment Groups
- **Control Group (50%)**: Current algorithm focusing on related genres (85% similar, 15% adjacent)
- **Treatment A (25%)**: Moderate expansion (70% related, 20% adjacent, 10% unrelated)
- **Treatment B (25%)**: Bold expansion (60% related, 25% adjacent, 15% unrelated)

### Genre Classification
**Related Genres**: Direct sub-genres and closely associated styles
- Example: Pop → Indie Pop, Electropop, Dance Pop

**Adjacent Genres**: Genres with some musical or cultural overlap
- Example: Pop → R&B, Electronic, Alternative Rock

**Unrelated Genres**: Genres with minimal traditional overlap
- Example: Pop → Jazz, Classical, Heavy Metal, Folk

### Sample Size & Duration
- **Sample Size**: 1.5 million Discover Weekly active users
- **Statistical Power**: 80% power to detect 5% relative improvement
- **Duration**: 6 weeks (6 Discover Weekly cycles)
- **Minimum Detectable Effect**: 4% change in engagement metrics

## Implementation Details

### Algorithm Modifications
1. **Genre Expansion Engine**
   - Develop unrelated genre recommendation system
   - Quality scoring for cross-genre recommendations
   - User compatibility scoring for genre jumps

2. **Track Selection Strategy**
   - Maintain high-quality tracks regardless of genre
   - Ensure smooth transitions between unrelated genres
   - Include genre "bridge" tracks when possible

3. **Personalization Adjustments**
   - Weight unrelated tracks by user's historical genre exploration
   - Consider listening time patterns for genre placement
   - Account for user demographics and cultural context

### Quality Gates
- Minimum track quality score: 7/10
- Artist popularity threshold to ensure basic familiarity
- Exclude explicit content for users with family settings

## Risks & Mitigation

### Risk 1: User Rejection of Unrelated Content
**Risk**: High skip rates for unrelated genres, negative feedback
**Mitigation**:
- Gradual exposure increase over 6 weeks
- Smart placement of unrelated tracks (not first or last)
- User feedback collection and algorithm adjustment

### Risk 2: Discover Weekly Abandonment
**Risk**: Users stop engaging with feature entirely
**Mitigation**:
- Monitor weekly engagement rates closely
- Maintain 60% familiar content minimum
- Quick rollback capability if engagement drops >10%

### Risk 3: Algorithm Confusion
**Risk**: Unrelated genre exposure confuses recommendation engine
**Mitigation**:
- Separate tracking for exploration vs. preference learning
- Weight unrelated genre interactions differently
- Implement feedback isolation for experimental content

### Risk 4: Cultural Sensitivity
**Risk**: Inappropriate genre mixing across cultural boundaries
**Mitigation**:
- Cultural awareness in genre pairing
- Regional customization of "unrelated" definitions
- Community and expert review of genre combinations

## Success Criteria
- **Primary**: ≥8% increase in user engagement score
- **Secondary**: ≥10% increase in genre diversity scores
- **Guardrail**: Discover Weekly skip rate increase <20%
- **Long-term**: Sustained cross-genre exploration after experiment

## Measurement Approach

### Short-term Metrics (Weekly)
- Track skip rates by genre category
- Monitor playlist completion rates
- Measure time spent per track type

### Long-term Metrics (6-8 weeks post-experiment)
- Analyze user's saved music genre diversity
- Track cross-genre playlist creation
- Measure retention and continued exploration
