<?xml version="1.0" encoding="UTF-8" ?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0"
xmlns:model="http://www.ibm.com/xmlns/prod/scheduling/1.0/Model">
    <xsl:output method="text" omit-xml-declaration="yes" encoding="UTF-8" indent="yes" />
    <xsl:preserve-space elements="*"/>
    <xsl:template match="/">
<xsl:value-of select="//node()/." separator=" "/>
<xsl:text>&#xa;</xsl:text>
 <xsl:for-each select="//@*">
        <xsl:value-of select="concat( ., '')"/>
<xsl:text>&#xa;</xsl:text>
  </xsl:for-each>
</xsl:template>
</xsl:transform>
