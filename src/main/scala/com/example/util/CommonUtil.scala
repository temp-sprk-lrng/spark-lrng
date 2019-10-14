package com.example.util

object CommonUtil {
  private var _baseFileLocation: String = null;

  def baseFileLocation: String = {
    if (_baseFileLocation == null) {
      throw new IllegalStateException("File location is not defined")
    }

    _baseFileLocation
  };

  def baseFileLocation(location: String): Unit = {
    _baseFileLocation = location
  }
}
