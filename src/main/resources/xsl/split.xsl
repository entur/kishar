<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:siri="http://www.siri.org.uk/siri"
                version="1.0">

    <xsl:output method="xml" indent="no" />

    <xsl:template match="/">
        <xsl:call-template name="Siri" />
    </xsl:template>

    <xsl:template name="Siri" match="/siri:Siri">

        <xsl:for-each select="/siri:Siri/siri:ServiceDelivery/siri:VehicleMonitoringDelivery/siri:VehicleActivity">
            <Siri xmlns="http://www.siri.org.uk/siri" version="2.0">
                <ServiceDelivery>
                    <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:ResponseTimestamp"></xsl:copy-of>
                    <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:ProducerRef"></xsl:copy-of>
                    <VehicleMonitoringDelivery version="2.0">
                        <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:VehicleMonitoringDelivery/siri:ResponseTimestamp"></xsl:copy-of>
                        <xsl:copy-of select="."></xsl:copy-of>
                    </VehicleMonitoringDelivery>
                </ServiceDelivery>
            </Siri>
        </xsl:for-each>

    </xsl:template>

</xsl:stylesheet>
