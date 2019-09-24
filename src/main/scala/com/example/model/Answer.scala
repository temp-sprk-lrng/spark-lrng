package com.example.model

import java.sql.Timestamp

case class Answer(id: Long,
                  ownerUserId: Long,
                  creationDate: Timestamp,
                  partnerId: Long,
                  score: Long,
                  body: String)
