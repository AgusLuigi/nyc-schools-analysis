def agent_suport_hotel(TEXT):
    import os
    import uuid
    from dotenv import load_dotenv
    from langfuse.openai import openai
    from langfuse import get_client, observe
    from pydantic import BaseModel

    load_dotenv()

    openai.api_key = os.getenv("OPENAI_API_KEY")
    openai.langfuse_public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    openai.langfuse_secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    openai.langfuse_host = os.getenv("LANGFUSE_BASE_URL")
    openai.langfuse_enabled = True
    model = "gpt-4.1-mini"

    langfuse = get_client()

    SYSTEM_PROMPT = "You are a helpful customer support agent that support tenants with their issues. Once a question comes up, answer them in a friendly and professional tone. Please also categorize the inquiry in one of these 4 sections: repair-request, complaint, payment-issue, general-question. Return the response and the category in a structured format"

    class ResponseModel(BaseModel):
        answer: str
        category: str

    class SupportBot:
        def __init__(self):
            self.question_count = 0

        def welcome(self):
            print("AI Support Bot")
            print(f"Model: {model}")
            print("\nTry asking:")
            print("- I can't log into my account!")
            print("- My payment failed")
            print("- How do I reset my password")

        @observe(name="Support Bot")
        def ask_support(self, question, session_id):
            self.question_count += 1

            langfuse.update_current_trace(session_id=session_id)

            response = openai.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": question}
                ],
                response_format=ResponseModel
            )

            # Extract metrics
            result = response.choices[0].message
            answer = result.parsed.answer
            category = result.parsed.category

            langfuse.update_current_trace(tags=[category])

            tokens = response.usage.total_tokens

            return {
                "answer": answer,
                "category": category,
                "tokens": tokens
            }

        def show_response(self, result):
            print(f"\nAI Response:")
            print(result["answer"])
            print(f"\nCategory: {result["category"]}")
            print(f"Tokens used: {result["tokens"]}")

        def run(self):
            self.welcome()
            session_id = uuid.uuid4().hex[:8]
            print(f"Session ID: {session_id}")
            
            while True:
                print("\nAsk a support question (or 'quit')")
                question = input("> ").strip()

                if question.lower() in ["quit", "exit", "q"]:
                    break

                try:
                    print(f"\nProcessing...")
                    result = self.ask_support(question, session_id)
                    self.show_response(result)
                except Exception as e:
                    print(f"Error: {e}")

            print(f"Asked {self.question_count} questions.")

    if __name__ == "__main__":
        bot = SupportBot()
        bot.run()