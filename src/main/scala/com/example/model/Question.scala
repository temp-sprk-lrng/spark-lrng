package com.example.model

import java.sql.Timestamp

case class Question(id: Long,
                    ownerUserId: Long,
                    creationDate: Timestamp,
                    score: Long,
                    title: String,
                    body: String)
