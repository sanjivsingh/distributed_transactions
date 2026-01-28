https://blog.hotstar.com/building-pubsub-for-50m-concurrent-socket-connections-5506e3c3dabf

- SMPP , CIMD
- SMTP - not a binary protocal, text
Kennel


Why to persist data if it has a low TTL?S3 reads?Edge caching, faster response time, lower costIf the score card data is changing frequently - wouldn’t we need to update the data at cdn frequentlySome AoF databaseReducing load on backend serversYesEvery connection may have different speed, wouldn’t it be a good strategy to cache for a short amount of period ?Wondering why so, why can’t we cache the scores, missed that part? 🤔though scorecard is keep changing, CDN still can help to avoid reading from source for every user to one read from source from the CDN's data centre


If S3 is expensive, Wouldn’t CDN make you cry?  CDN cheaper than s3

what was the conclusion on persistent connections vs polling for scorecard?

polling solution (with CDN ) is good for scorecard IMO. Since data is not unique per user, I don't think persistent connections (ws, sse, etc) adds any value other than complexity/cost.


https://videojs.org/advanced/?video=big-buck-bunny


https://d2zihajmogu5jn.cloudfront.net/big-buck-bunny/rendition/1719543778001_4328560526001_s-1.ts?pubId=1719543778001&videoId=4328294689001

- HLS


https://openconnect.netflix.com/en/?utm_referrer=https://www.google.com/


What's the break-even point where building your own CDN becomes cheaper than using commercial CDNs?

Does the routing service/front end keep the metadata live users (isp, geography etc) or decision making is heuristic based?

Pseudo code

​step 1: client knows its not getting any response from current cdn
Step 2: client reaches out to rerouting service to ask for a new cdn
Step 3: re routing service looks up something( what is this something, cdn level stats)?  & return appropriate cdn

Also is re routing service completely cache free? Can it also have a cdn in front of it?


https://chatgpt.com/share/696132aa-c0ec-8012-890f-cbe7319d7928

https://www.youtube.com/watch?v=SlLD-gvX7nM

Server guided ads

Mux = 

https://data.mux.com/ - observability 

TLS - certificate authority 

https://www.linkedin.com/posts/stephwangbuilds_a-few-things-ive-come-to-appreciate-over-activity-7413591289046405120-Iz7b?utm_source=share&utm_medium=member_android&rcm=ACoAAA_FhPsBS1iOIiWHZs-bvwwm5BHPnrazGNU


https://blog.hotstar.com/building-pubsub-for-50m-concurrent-socket-connections-5506e3c3dabf

