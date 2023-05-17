from dotenv import load_dotenv, find_dotenv
import os
import pprint
from datetime import datetime as dt
from pymongo import MongoClient

connection_string = 'mongodb://localhost:27017'

client = MongoClient(connection_string)

dbs = client.list_database_names()

production = client.production


def create_student_collection():
    student_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name", "year", "major"],
            "properties": {
                "name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "year": {
                    "bsonType": "int",
                    'minimum': 2017,
                    "maximum": 3017,
                    "description": "must be an integer in [ 2017, 3017 ] and is required"
                },
                "major": {
                    "enum": ["Math", "English", "Computer Science", "History", None],
                    "description": "can only be one of the enum values and is required"
                },
                "gpa": {
                    "bsonType": ["double"],
                    "description": "must be a double if the field exists"
                }
            }
        }
    }

    try:
        # Create Collection Student
        production.create_collection("student")
    except Exception as e:
        print(e)

        # Creates validation for a student
    production.command("collMod", "student", validator=student_validator)


def create_teacher_collection():
    teacher_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["firstname", "lastname", "date_of_birth"],
            "properties": {
                "firstname": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "lastname": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
            }
        }
    }

    try:
        # Create Collection Student
        production.create_collection("teacher")
    except Exception as e:
        print(e)

        # Creates validation for a student
    production.command("collMod", "teacher", validator=teacher_validator)


# create_teacher_collection()

def create_data():

    teachers = [
        {"firstname": "Jack", "lastname":"Wall", "date_of_birth": dt(2000, 7, 20)},
        {"firstname": "Mark", "lastname": "Shultz", "date_of_birth": dt(1979, 4, 30)},
        {"firstname": "Jesse", "lastname": "Walburg", "date_of_birth": dt(1984, 1, 22)},
        {"firstname": "Caleb", "lastname": "Austin", "date_of_birth": dt(1977, 10, 18)}
    ]

    teacher_collection = production.teacher
    teachers = teacher_collection.insert_many(teachers).inserted_ids

    students = [
        {"name": "Anne", "year": 2017, "major": "English", "gpa": 4.0},
        {"name": "Jake", "year": 2018, "major": "Math", "gpa": 3.7},
        {"name": "Rachael", "year": 2022, "major": "Computer Science", "gpa": 3.5},
        {"name": "Jhin", "year": 2022, "major": "History", "gpa": 3.5},
    ]