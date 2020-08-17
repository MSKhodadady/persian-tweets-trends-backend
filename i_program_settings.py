"""
This module provides interface for accessing program settings
"""
import json
import os


def get_settings(setting_name):
    """
    gets a settings by its name
    """
    with open(os.path.dirname(__file__) + '/program-settings.json', 'r') as settings:
        return json.load(settings)[setting_name]

def get_all_settings():
    """
    returns all settings
    """
    with open(os.path.dirname(__file__) + '/program-settings.json', 'r') as settings:
        return json.load(settings)
