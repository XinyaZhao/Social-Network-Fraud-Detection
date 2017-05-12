/**
 * Created by yunjianyang on 11/11/16.
 */

import java.io.*;
import java.net.ResponseCache;

import facebook4j.*;
import facebook4j.conf.*;
import facebook4j.auth.*;
public class GetFacebookData {

    public static  Configuration createConfiguration() {
        ConfigurationBuilder confBuilder = new ConfigurationBuilder();

        confBuilder.setDebugEnabled(true);
        confBuilder.setOAuthAppId("1954498418110940");
        confBuilder.setOAuthAppSecret("1ea7c3608af8f153db3e15a89d8102ed");
        confBuilder.setOAuthPermissions("email,publish_stream,...");
        confBuilder.setUseSSL(true);
        confBuilder.setJSONStoreEnabled(true);
        Configuration configuration = confBuilder.build();
        return configuration;
    }

    public static void main(String [] args){
        /* generage a facebook client and fetch a access token*/
        Configuration configuration =  createConfiguration();
        FacebookFactory facebookFactory = new FacebookFactory(configuration );
        Facebook facebookClient = facebookFactory.getInstance();
        AccessToken accessToken = null;
        try{
            OAuthSupport oAuthSupport = new OAuthAuthorization(configuration );
            accessToken = oAuthSupport.getOAuthAppAccessToken();
        }catch (FacebookException e) {
            System.err.println("Error while creating access token " + e.getLocalizedMessage());
        }
        facebookClient.setOAuthAccessToken(accessToken);


        /* use api to get data*/
        char ch = 'a';
        int index = -1;
        int loop = 0;
        StringBuilder SearchKey = new StringBuilder("");
        for(int j = 1; j < 100; j ++){
            if('a' < ch && ch < 'z'){
                SearchKey.setCharAt(index, ch);
                ch ++;
            }
            if('a' == ch){
                SearchKey.append(ch);
                index ++;
                ch ++;
            }
            if('z' == ch){
                SearchKey.setCharAt(index, ch);
                ch = 'a';
            }
            String s = new String(SearchKey);
            BufferedWriter bufferedWriter = null;
            try {
                bufferedWriter = new BufferedWriter(new FileWriter("/Users/yunjianyang/Desktop/Output.txt", true));
                try {
                    ResponseList<Page> pagelst = facebookClient.searchPages(s);
                    for (Page crpg : pagelst) {
                        //System.out.println("current page: " + crpg.getName());
                        ResponseList<Like> likes = facebookClient.getUserLikes(crpg.getId());
                        for (int i = 0; i < likes.size(); i++) {
                            //System.out.println(crpg.getId() + " " + likes.get(i).getId());
                            bufferedWriter.write(crpg.getId() + " " + likes.get(i).getId());
                            bufferedWriter.newLine();
                        }
                    }
                    try {
                        System.out.println(loop++);
                        Thread.sleep(12000);    //About 2 minutes, 1000 milliseconds is one second.
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                } catch (FacebookException e) {
                    System.err.println(e);
                    try {
                        Thread.sleep(12000);    //About 2 minutes, 1000 milliseconds is one second.
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }catch (IOException e){
                System.err.println(e);
            } finally
            {
                //Closing the BufferedWriter
                try
                {
                    if (bufferedWriter != null)
                    {
                        bufferedWriter.flush();
                        bufferedWriter.close();
                    }
                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                }
            }
        }
    }
}
