package org.qbproject.csv

import org.qbproject.api.csv.QBResource

case class CSVErrorInfo(resource: QBResource, csvRow: Int)
