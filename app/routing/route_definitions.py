"""Route definitions with example utterances for semantic routing."""

from dataclasses import dataclass, field
from typing import List


@dataclass
class Route:
    """A semantic route with example utterances."""

    name: str
    description: str
    utterances: List[str] = field(default_factory=list)
    maps_to: str = "needs_tools"  # needs_tools, can_answer, needs_clarification


# Define all routes with example utterances
ROUTES = [
    Route(
        name="data_query",
        description="Questions about analysis results, rankings, data values",
        maps_to="needs_tools",
        utterances=[
            # Ranking queries
            "What are the top ranked wards",
            "Show me the highest risk areas",
            "Which wards have the highest vulnerability",
            "What are the lowest scoring wards",
            "List the top 10 high risk wards",
            "Which areas have the most malaria cases",
            "Show me the bottom ranked wards",
            "What wards are most at risk",
            "Rank the wards by risk score",
            "Which LGAs have the highest incidence",
            # Result interpretation
            "Why is Kabuga ranked so high",
            "What does my data show",
            "Summarize the analysis results",
            "What are the key findings",
            "Explain the results",
            "What patterns do you see in my data",
            "Tell me about my analysis results",
            "What does the risk score mean for this ward",
            "Why are these wards flagged as high risk",
            "How did you calculate this score",
            # Data queries
            "What variables are in my data",
            "Show me the data summary",
            "What's the average TPR",
            "How many wards are in my dataset",
            "What's the range of values for incidence",
            "Show me the distribution of scores",
            "What columns are available",
            "Describe my dataset",
            "What is the mean vulnerability score",
            "Show me statistics for my data",
        ],
    ),
    Route(
        name="visualization",
        description="Requests for maps, charts, and visual outputs",
        maps_to="needs_tools",
        utterances=[
            # Map requests
            "Create a vulnerability map",
            "Show me the risk map",
            "Generate a choropleth map",
            "Map the high risk areas",
            "Create a map of TPR values",
            "Show the geographic distribution",
            "Visualize the risk scores on a map",
            "Generate a heat map",
            "Show me a ward-level map",
            "Create a map showing the results",
            # Chart requests
            "Create a bar chart of the rankings",
            "Show me a histogram of TPR",
            "Generate a box plot",
            "Create a scatter plot",
            "Plot the distribution",
            "Show me a pie chart",
            "Visualize the correlation",
            "Create a trend chart",
            "Generate a comparison chart",
            "Plot the top 10 wards",
            # General visualization
            "Visualize my data",
            "Show me a graph",
            "Create a chart",
            "Generate a visualization",
            "Plot this data",
        ],
    ),
    Route(
        name="analysis",
        description="Requests to run analysis, compute scores, process data",
        maps_to="needs_tools",
        utterances=[
            # Risk analysis
            "Run the malaria risk analysis",
            "Analyze my data",
            "Calculate risk scores",
            "Run the vulnerability analysis",
            "Perform a comprehensive analysis",
            "Compute the composite scores",
            "Start the analysis",
            "Run complete malaria analysis",
            "Analyze the uploaded data",
            "Process my data and calculate risks",
            # Specific analyses
            "Run PCA on my data",
            "Calculate correlation coefficients",
            "Perform cluster analysis",
            "Run statistical analysis",
            "Calculate descriptive statistics",
            "Compute spatial autocorrelation",
            "Run regression analysis",
            "Calculate the vulnerability index",
            "Perform hotspot analysis",
            "Analyze trends in my data",
            # Data quality
            "Check data quality",
            "Validate my dataset",
            "Check for missing values",
            "Assess data completeness",
            "Verify the data",
        ],
    ),
    Route(
        name="itn_planning",
        description="Bed net distribution and intervention planning",
        maps_to="needs_tools",
        utterances=[
            # Direct ITN requests
            "Plan bed net distribution",
            "How many nets do I need",
            "Create an ITN distribution plan",
            "Allocate bed nets by ward",
            "Plan LLIN distribution",
            "Calculate net requirements",
            "How should I distribute the nets",
            "Create a bed net allocation plan",
            "Plan mosquito net distribution",
            "Generate ITN recommendations",
            # With parameters
            "I have 50000 nets to distribute",
            "Plan distribution for 100000 LLINs",
            "Distribute 25000 bed nets across wards",
            "Allocate nets with average household size of 5",
            "Plan ITN distribution for Kano state",
            "How many nets for each LGA",
            # Intervention planning
            "Plan malaria interventions",
            "Prioritize areas for intervention",
            "Where should we focus interventions",
            "Create an intervention priority list",
            "Recommend intervention strategies",
        ],
    ),
    Route(
        name="workflow",
        description="TPR workflow commands and data pipeline operations",
        maps_to="needs_tools",
        utterances=[
            # TPR workflow
            "Start the TPR workflow",
            "Run TPR analysis",
            "Begin the TPR pipeline",
            "Execute TPR workflow",
            "Start malaria data workflow",
            "Run the complete workflow",
            "Initialize TPR analysis",
            "Start data processing workflow",
            # Data upload/export
            "Upload my data",
            "Export the results",
            "Download the analysis",
            "Save results to Excel",
            "Generate a report",
            "Export to CSV",
            "Create a PDF report",
            "Save my work",
            # Session management
            "Clear my session",
            "Reset the analysis",
            "Start fresh",
            "Load previous results",
        ],
    ),
    Route(
        name="knowledge",
        description="General malaria knowledge and methodology questions",
        maps_to="can_answer",
        utterances=[
            # Malaria basics
            "What is malaria",
            "How is malaria transmitted",
            "What causes malaria",
            "What are the symptoms of malaria",
            "How can malaria be prevented",
            "What is the malaria life cycle",
            "How do mosquitoes spread malaria",
            "What is Plasmodium",
            "What are Anopheles mosquitoes",
            "How does malaria affect the body",
            # Epidemiology
            "What is malaria epidemiology",
            "What is disease surveillance",
            "What is incidence rate",
            "What is prevalence",
            "How is malaria burden measured",
            "What are malaria risk factors",
            "What is vector control",
            "How do bed nets work",
            "What is indoor residual spraying",
            "What is the malaria situation in Nigeria",
            # Methodology
            "What is PCA analysis",
            "How does principal component analysis work",
            "What is a composite score",
            "How is vulnerability calculated",
            "What is spatial analysis",
            "What is a choropleth map",
            "How do you interpret risk scores",
            "What is stratification",
            "What is the scoring methodology",
            "How are wards ranked",
            # ChatMRPT help
            "What can you do",
            "How do I use this tool",
            "What analyses are available",
            "Help me understand the system",
            "What data format do you need",
        ],
    ),
    Route(
        name="greeting",
        description="Greetings, thanks, and conversational responses",
        maps_to="can_answer",
        utterances=[
            # Greetings
            "Hello",
            "Hi",
            "Hey",
            "Good morning",
            "Good afternoon",
            "Good evening",
            "Greetings",
            "Howdy",
            "Hi there",
            "Hello there",
            # Thanks
            "Thank you",
            "Thanks",
            "Thanks a lot",
            "Thank you so much",
            "Much appreciated",
            "Thanks for your help",
            "Great, thanks",
            # Acknowledgments
            "Okay",
            "OK",
            "Sure",
            "Yes",
            "No",
            "Got it",
            "I understand",
            "Alright",
            "Fine",
            # Farewells
            "Bye",
            "Goodbye",
            "See you",
            "Take care",
            "That's all",
            "I'm done",
        ],
    ),
]


def get_route_by_name(name: str) -> Route | None:
    """Get a route by its name."""
    for route in ROUTES:
        if route.name == name:
            return route
    return None


def get_all_utterances() -> dict[str, list[str]]:
    """Get all utterances grouped by route name."""
    return {route.name: route.utterances for route in ROUTES}
