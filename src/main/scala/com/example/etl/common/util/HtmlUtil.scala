package com.example.etl.common.util

import org.jsoup.Jsoup
import org.jsoup.select.Elements

object HtmlUtil {
  def extractLinkBody(html: String): Elements = Jsoup.parse(html).select("a[href]")

  def containsLinks(html: String): Boolean = !extractLinkBody(html).isEmpty

  def extractCodeBody(html: String): Elements = Jsoup.parse(html).select("pre").select("code")

  def containsCode(html: String): Boolean = !extractCodeBody(html).isEmpty

  def codeLen(html: String): Int = extractCodeBody(html).html.split("\\r\\n|\\r|\\n").length
}
