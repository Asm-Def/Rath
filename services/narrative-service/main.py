import sys, os
from mangum import Mangum
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from insight_flask import app
handler = Mangum(app)