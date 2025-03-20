from PyQt5.QtWidgets import (QApplication, QWidget, QTextEdit, QLineEdit, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, QMessageBox, QComboBox)
from PyQt5.QtCore import pyqtSignal, Qt

# GUI class for user-agent interaction
import json

class CHIMPInterface(QWidget):
    approved_signal = pyqtSignal(str)

    def __init__(self, agent, initial_request, initial_instructions, json_file_path):
        super().__init__()
        self.agent = agent
        self.latest_response = ""
        
        self.initial_request = initial_request
        self.initial_instructions = initial_instructions
        
        
        
        self.json_file_path = json_file_path
        self.json_data = self.load_json()
        self.setWindowTitle(f"Chat with {self.agent.agent_name}")
        self.setGeometry(100, 100, 600, 400)
        self.init_ui()

    def load_json(self):
        try:
            with open(self.json_file_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load JSON file: {e}")
            return {}

    def save_json(self):
        try:
            with open(self.json_file_path, 'w') as file:
                json.dump(self.json_data, file, indent=4)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save JSON file: {e}")

    def init_ui(self):
        layout = QVBoxLayout()
        
        # Current agent label
        agent_name_heading = QLabel("Agent:")
        agent_name_heading.setStyleSheet("font-weight: bold; font-size: 16px;")
        self.agent_name_label = QLabel()
        self.agent_name_label.setText(f"{self.agent.agent_name}")
        self.agent_name_label.setStyleSheet("font-size: 16px;")

        # Top layout for the "Approved" button
        top_layout = QHBoxLayout()
        self.approve_button = QPushButton("Approved")
        self.approve_button.setFixedWidth(100)
        self.approve_button.clicked.connect(self.on_approved_clicked)
        top_layout.addWidget(agent_name_heading)
        top_layout.addWidget(self.agent_name_label)
        top_layout.addWidget(self.approve_button, alignment=Qt.AlignRight)
        layout.addLayout(top_layout)

        # Chat display area
        self.text_area = QTextEdit(self)
        self.text_area.setReadOnly(True)
        self.text_area.setStyleSheet("""
            QTextEdit {
                border: 2px solid #cccccc;
                border-radius: 10px;
                padding: 10px;
                font-size: 16px;   
            }
        """)
        layout.addWidget(self.text_area)

        # User input area
        self.user_input = QLineEdit(self)
        self.user_input.setPlaceholderText("Type your message and press Enter")
        self.user_input.setStyleSheet("""
            QLineEdit {
                padding: 10px;
                border: 2px solid #cccccc;
                border-radius: 10px;
                font-size: 16px;
            }
        """)
        self.user_input.returnPressed.connect(self.on_enter_pressed)
        layout.addWidget(self.user_input)
                
        # QComboBox
        self.dropdown_box = QComboBox()
        layout.addWidget(self.dropdown_box)
        
        # Save and open file buttons
        self.save_button = QPushButton("Save Conversation")
        self.save_button.clicked.connect(self.on_save_button_clicked)
        self.add_to_json_button = QPushButton("Add to JSON")
        self.add_to_json_button.clicked.connect(self.on_add_to_json_clicked) 
        
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.add_to_json_button)
        button_layout.addWidget(self.save_button)
        layout.addLayout(button_layout)
        
        # Set layout
        self.setLayout(layout)
        self.text_area.append(f"Initial Instructions: {self.initial_instructions}")
        self.text_area.append("\n")
        self.text_area.append(f"Initial Request: {self.initial_request}")
        self.text_area.append(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        self.get_agent_response(self.initial_request)

    def get_agent_response(self, prompt):
        self.user_input.setEnabled(False)
        response = self.agent.get_response(prompt)
        self.display_agent_response(response)

    def display_agent_response(self, response):
        self.latest_response = response
        self.text_area.append(f"{self.agent.agent_name}: {response}")
        self.text_area.append("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        self.user_input.setEnabled(True)

    def on_enter_pressed(self):
        user_text = self.user_input.text().strip()
        if user_text:
            self.text_area.append(f"User: {user_text}") 
            self.text_area.append(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            self.user_input.clear()
            self.get_agent_response(user_text)
            
            
            # add value to combobox
            self.dropdown_box.addItem(f"{user_text}")
            
        
            
        
    def on_approved_clicked(self):
        self.approved_signal.emit(self.latest_response)
        self.close()
    
    def copy_latest_answer(self):
        clipboard = QApplication.clipboard()
        clipboard.setText(self.latest_response)
        self.text_area.append("Latest answer copied to clipboard.")
    
    def on_save_button_clicked(self):
        text = self.text_area.toPlainText()
        if not text:
            QMessageBox.warning(self, 'Warning', 'Text field is empty')
            return

        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getSaveFileName(self, 'Save File', '', 'Text Files (*.txt);;All Files (*)', options=options)
        if fileName:
            try:
                with open(fileName, 'w') as file:
                    file.write(text)
                QMessageBox.information(self, 'Success', 'File saved successfully')
            except Exception as e:
                QMessageBox.critical(self, 'Error', f'Could not save file: {e}')
    
    def get_current_task_index(self):
        # search for prompt in tasks
        for i in range(len(self.json_data['TASKS'])):
            if self.json_data['TASKS'][i]['request'] == self.initial_request:
                return i
        # return -1 if not found
        return -1
        
    
    def on_add_to_json_clicked(self):
        # Get the selected value from the QComboBox
        selected_value = self.dropdown_box.currentText()
        if not selected_value:
            QMessageBox.warning(self, "Warning", "No value selected from the dropdown.")
            return

        # Find the task to update
        task_updated = False
        for task in self.json_data["TASKS"]:
            if task['request'] == self.initial_request:
                # Update the task's request
                task['request'] = f"{self.initial_request} \n\n{selected_value}"
                task_updated = True
                break

        if task_updated:
            # Save the updated JSON
            self.save_json()

            # Reload the JSON and ensure UI components reflect the changes
            self.json_data = self.load_json()
            self.text_area.append("Task updated and saved to JSON.")
            self.update_ui_after_json_save()
        else:
            QMessageBox.warning(self, "Warning", "Failed to update the task in JSON.")


    def update_ui_after_json_save(self):
        """Refresh UI components based on the updated JSON"""
        current_task_index = self.get_current_task_index()
        if current_task_index != -1:
            updated_task = self.json_data["TASKS"][current_task_index]
            self.text_area.append(f"Updated Task: {updated_task['request']}")