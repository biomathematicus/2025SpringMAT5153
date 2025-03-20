import os
import openai
import anthropic

"""
This file holds all the code for the generation of the different agents we will be using. Utilizes a design pattern
to generate agents based on request

1. OpenAI
2. Antrhopic
3. Grok
"""
class BaseAgent:
    def __init__(self, model, config):
        self.agent_name = model["agent_name"]
        self.model_code = model['model_code']
        self.model_name = model['model_name']
        self.temperature = model['temperature']
        self.general_instructions = config["general_instructions"]

class OpenAIChatbot(BaseAgent):
    def __init__(self, model, config):
        super().__init__(model, config)
        openai.api_key = os.getenv("OPENAI_API_KEY")
        if not openai.api_key:
            print("API key is not set. Please set the OPENAI_API_KEY environment variable.")
            exit(1)

        self.client = openai.OpenAI()
        self.assistant = self.client.beta.assistants.create(
            model=self.model_code,
            instructions=self.general_instructions,  
            name=self.agent_name,
            tools=[{"type": "file_search"}]
        )
        self.thread = self.client.beta.threads.create()

    def get_response(self, prompt):
        try:
            self.client.beta.threads.messages.create(
                thread_id=self.thread.id,
                role="user",
                content=prompt,
            )
            my_run = self.client.beta.threads.runs.create(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                model=self.assistant.model,
                temperature=self.temperature
            )
            while my_run.status in ["queued", "in_progress"]:
                my_run = self.client.beta.threads.runs.retrieve(
                    thread_id=self.thread.id,
                    run_id=my_run.id
                )
            if my_run.status == "completed":
                all_messages = self.client.beta.threads.messages.list(
                    thread_id=self.thread.id
                )
                for message in all_messages.data:
                    if message.role == "assistant":
                        s = message.content[0].text.value.strip()
                        s = s.replace("```latex", "").replace("```", "")
                        return s
            return "Error: Could not complete the request."
        except Exception as e:
            return f"Error: {e}"

class ClaudeAgent(BaseAgent):
    def __init__(self, model, config):
        super().__init__(model, config)
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.system_prompt = self.general_instructions
        self.conversation_history = []

    def get_response(self, prompt):
        try:
            self.conversation_history.append({"role": "user", "content": prompt})
            response = self.client.messages.create(
                model=self.model_code,
                max_tokens=1000,
                temperature=self.temperature,
                system=self.system_prompt,
                messages=self.conversation_history
            )
            assistant_message = response.content[0].text
            self.conversation_history.append({"role": "assistant", "content": assistant_message})
            return assistant_message.strip()
        except Exception as e:
            return f"Error: {e}"

'''
TODO implement Grok code here
'''
