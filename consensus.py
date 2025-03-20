from agents import OpenAIChatbot, ClaudeAgent
from interface import CHIMPInterface
import os
import sys
from PyQt5.QtWidgets import QApplication
import json


def load_config(file_name):
    with open(file_name, 'r') as file:
        return json.load(file)

     
def main(file_name):
    # Create task manager
    # task_manager = TaskPipelineManager(file_name)
    
    config_data = load_config(file_name)
    models = config_data["MODELS"]
    tasks = config_data["TASKS"]
    config = config_data["CONFIG"]
    
    agents = []
    for model in models:
        if "claude" in model["model_name"].lower():
            agent = ClaudeAgent(model, config)
        else:
            agent = OpenAIChatbot(model, config)
        agents.append(agent)
    

    harmonizer_model = {
        "agent_name": config["harmonizer_name"],
        "model_code": config["harmonizer_code"],
        "model_name": config["harmonizer_name"],
        "temperature": config["harmonizer_temperature"]
    }
    harmonizer_agent = OpenAIChatbot(harmonizer_model, config)

    app = QApplication(sys.argv)
    
        
    
    for k, task in enumerate(tasks):
        for i, agent in enumerate(agents):
            # Reload configuration for each agent-task pair
            updated_config = load_config(file_name=file_name)
            updated_tasks = updated_config["TASKS"]
            
            if len(updated_tasks) <= k:  # Ensure task exists
                raise ValueError(f"Task index {k} out of range in `updated_tasks`")

            initial_request = updated_tasks[k]['request']
            initial_instructions = updated_tasks[k]['instructions']

            # Initialize the interface for the current agent and task
            chimp_interface = CHIMPInterface(
                agent=agent,
                initial_request=initial_request,
                initial_instructions=initial_instructions,
                json_file_path=file_name
            )

            initial_responses = []

            def on_response_approved(response):
                initial_responses.append(response)

            # Connect the approval signal
            chimp_interface.approved_signal.connect(on_response_approved)
            chimp_interface.show()
            app.exec_()
            
            
        # Step 2: Each agent critiques other agents' responses
        critiqued_responses = [[None for _ in range(len(agents))] for _ in range(len(agents))]
        for i, agent in enumerate(agents):
            for j, other_response in enumerate(initial_responses):
                if i != j:
                    critique_prompt = f"Another LLM responded to the same question as follows. Find the flaws:\n\n{other_response}"
                    critiqued_responses[i][j] = agent.get_response(critique_prompt)
        
        # Step 3: Each agent refines its response
        refined_responses = []
        for i, agent in enumerate(agents):
            critiques_for_agent = "\n\n".join([f"Criticism from another agent:\n{critiqued_responses[j][i]}" for j in range(len(agents)) if j != i])
            refine_prompt = f"Other agents criticized your response as follows. Do not lose information in summarization; keep all relevant details, including examples, source code, etc. Validate criticism and refine as needed. If there here are no specific criticisms provided by other agents, then respond with your latest most complete answer:\n\n{critiques_for_agent}"
            refined_responses.append(agent.get_response(refine_prompt))
        
        # Step 4: Each agent harmonizes refined responses
        harmonized_responses = refined_responses
        # harmonized_responses = []
        # for i, agent in enumerate(agents):
        #     combined_responses = "\n\n".join([f"Refined response from another agent:\n{resp}" for resp in refined_responses])
        #     harmonize_prompt = f"The following are refined responses from different agents. Harmonize these responses to produce a single unified version of the task:\n\n{combined_responses}"
        #     harmonized_responses.append(agent.get_response(harmonize_prompt))

        # Step 5: Harmonizer agent creates a single output
        combined_harmonized_responses = "\n\n".join([f"Harmonized response from agent {i+1}:\n{resp}" for i, resp in enumerate(harmonized_responses)])
        final_harmonization_prompt = f"This is the Harmonizaiton Step. The following are responses from different agents. Produce a single unified and improved version:\n\n{combined_harmonized_responses}"
        final_harmonization_instructions = config['general_instructions']
        
        
        harmonizer_interface = CHIMPInterface(harmonizer_agent,
                                              final_harmonization_prompt,
                                              final_harmonization_instructions,
                                              file_name)
        
        # def on_harmonizer_approved(response):
        #     with open(task_manager.tasks['file_name'], 'a') as f:
        #         f.write(response)
        def on_harmonizer_approved(response):
            pass

        harmonizer_interface.approved_signal.connect(on_harmonizer_approved)
        harmonizer_interface.show()
        app.exec_()

if __name__ == "__main__":
    main(f"{os.getcwd()}/knowledge_base_transcriptomic.json")    