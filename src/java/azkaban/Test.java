package azkaban;

/**
 * @author zhongshu
 * @since 2017/12/22 下午4:37
 */
public class Test {

    public static void main(String[] args) {

        String s = "v2\t[12/Jan/2018:08:00:01 +0800]\t222.185.122.34\t1801120759279764\tc=14908&v1=21381&v2=21408&p=oad2&loc=CN3204&adstyle=oad&ac=20540&ad=389917&pt=25049&b=457027&bk=108749998&at=15&spead=1&du=238&adtime=30&trule=36841&mx=2&al=9392826&out=0&au=1&vid=93064260&tvid=93064260&rt=968a6fde7d840d523fa30c21f42a059b&spead=1&uv=15157150376292541037&uuid=279e1c51-7a25-935c-514c-000000000160e7a7f472c71e&UUID=279e1c51-7a25-935c-514c-000000000160e7a7f472c71e&vt=pgc&rd=my.tv.sohu.com&fee=0&isIf=0&suv=1801120759279764&uid=15157150376292541037&myTvUid=322921746&sz=783_583&md=tAJ4SC0r0ZEiH/3644jFWGxDf2G0xMlc151&crid=0&scookie=2&ugcode=MT-nD0sSGzgTtc4kqUlzvaka34T3D5uZq4n1OA1djfh0jYZ1HdINTao3C0t1juGWqfXJoRgJOOT3uqB-XZAc_AKeYwL-Pq4jsgHr&ugu=322921746&ar=0&sign=bbzqFXgZDY6Cq1UoxGRuD2BhklNkkwIG036fuP7I-0nGjqw_Vk0gzUvsPo_jIqStSo2S0LnEe_o.&rip=222.185.122.34&sip=10.13.91.50&fip=222.185.122.34&url=http%3A//my.tv.sohu.com/us/322921746/93064260.shtml&ti=5Zub5Liq6aKg6KaG5oOz6LGh55qE5oKs55aR55S15b2x77yM5q+U56We5o6i5aSP5rSb5YWL6L+Y54On6ISR55qE5aSn54mH77yM5L2g55yL6L+H5ZOq5Liq&tag=IA==&plat=pc&adplat=0&v1=21381&v2=21408&offline=0&endtime=20180331&ad=389917&b=457027&bk=108749998&pagetype=1&dspid=10056&suid=1801120759279764&seq=3&w=660&h=508&cheattype=0&bidid=e16174bd0a9f4f568260f3c3aba457ff&sperotime=1515715169&template=normal,null&platsource=tvpc&vp=e&encrypt=3rHKP5769Tfqdr0_NfqltzJYFGoRw0C8juDi3lwvSrSjpBMx\t\"-\"\t\"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)\"\t10.10.64.155\t-\t204";
        System.out.println();
        System.out.println(s.split("\\t")[4]);;
        System.out.println();
    }
}
