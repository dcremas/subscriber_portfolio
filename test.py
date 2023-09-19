import sys
import unittest
import sqlite3
from datetime import datetime
import pandas as pd


def main(out = sys.stderr, verbosity = 2):
    loader = unittest.TestLoader()
  
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity = verbosity).run(suite)


date_string = datetime.now().strftime("%Y-%m-%d")

students_sql = "SELECT DISTINCT * FROM cademycode_students;"
courses_sql = "SELECT DISTINCT * FROM cademycode_courses;"
student_jobs_sql = "SELECT DISTINCT * FROM cademycode_student_jobs;"

students_columns = ['uuid', 'name', 'dob', 'sex',
                    'contact_info', 'job_id', 'num_course_taken',
                    'current_career_path_id', 'time_spent_hrs']

courses_columns = ['career_path_id', 'career_path_name', 'hours_to_complete']

student_jobs_columns = ['job_id', 'job_category', 'avg_salary']

class TestDatabase(unittest.TestCase):

    @classmethod
    def setUp(self):
        # Load the new data and also the existing data from the two databases.
        conn_new = sqlite3.connect("subscriber-pipeline-starter-kit/dev/cademycode_updated.db")
        self.df_students_new = pd.read_sql_query(students_sql, conn_new)
        self.df_courses_new = pd.read_sql_query(courses_sql, conn_new)
        self.df_student_jobs_new = pd.read_sql_query(student_jobs_sql, conn_new)
                       
    def test_record_count_students(self):
        self.assertGreater(len(self.df_students_new), 0, msg="No records within the students table.")

    def test_record_count_courses(self):
        self.assertGreater(len(self.df_courses_new), 0, msg="No records within the courses table.")

    def test_record_count_student_jobs(self):
        self.assertGreater(len(self.df_student_jobs_new), 0, msg="No records within the student jobs table.")

    def test_columns_students(self):
        for field in students_columns:
            with self.subTest(field):
                self.assertIn(field, self.df_students_new.columns, msg="This column does not exist.")

    def test_columns_courses(self):
        for field in courses_columns:
            with self.subTest(field):
                self.assertIn(field, self.df_courses_new.columns, msg="This column does not exist.")

    def test_columns_student_jobs(self):
        for field in student_jobs_columns:
            with self.subTest(field):
                self.assertIn(field, self.df_student_jobs_new.columns, msg="This column does not exist.")


if __name__ == '__main__':
    with open(f"logs/testing/{date_string}_testing.log", 'a') as f:
        main(f)
