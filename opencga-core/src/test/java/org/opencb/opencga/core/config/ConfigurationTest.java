/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.config;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by imedina on 16/03/16.
 */
@Category(ShortTests.class)
public class ConfigurationTest {

    @Test
    public void testEvents() throws InterruptedException {
        Observable<String> observable = Observable.just("hello");

        CompositeDisposable compositeDisposable = new CompositeDisposable();
        observable.subscribe(s -> System.out.println("Suscriptor 1: " + s), (t) -> System.out.println("error"), () -> System.out.println("Suscriptor 1 completed"), compositeDisposable);
        observable.doOnDispose(() -> {
            System.out.println("Esto es lo que hago al final");
        });
        observable.subscribe(s -> System.out.println("Suscriptor 2: " + s), (t) -> System.out.println("error"), () -> System.out.println("Suscriptor 2 completed"), compositeDisposable);
        observable.subscribe(s -> { throw new Exception("My exception"); }, throwable -> System.out.println(throwable.getMessage()), () -> {}, compositeDisposable);
        System.out.println(compositeDisposable.isDisposed());
        compositeDisposable.dispose();
        System.out.println(compositeDisposable.isDisposed());

        observable.subscribe(s -> System.out.println("Suscriptor 3: " + s), (t) -> System.out.println("error"), () -> System.out.println("Suscriptor 3 completed"), compositeDisposable);
        compositeDisposable.clear();
        Thread.sleep(2000);

//        Observer<OpencgaEvent> observer = new Observer<OpencgaEvent>() {
//            @Override
//            public void onSubscribe(@NonNull Disposable d) {
//                System.out.println("On subscription " + d);
//            }
//
//            @Override
//            public void onNext(OpencgaEvent opencgaEvent) {
//                System.out.println("On next " + opencgaEvent.getMessage());
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                System.out.println("On error " + throwable);
//            }
//
//            @Override
//            public void onComplete() {
//                System.out.println("On complete");
//            }
//        };
//        OpencgaEventManager.subscribe(OpencgaEventManager.SAMPLE_CREATE, observer);

//        OpencgaEventManager.addEvent(OpencgaEventManager.SAMPLE_CREATE, Observable.just(new OpencgaEvent("First event")));
//        OpencgaEventManager.addEvent(OpencgaEventManager.SAMPLE_CREATE, Observable.just(new OpencgaEvent("Second event")));
//        OpencgaEventManager.subscribe(OpencgaEventManager.SAMPLE_CREATE, observer);
//        OpencgaEventManager.addEvent(OpencgaEventManager.SAMPLE_CREATE, Observable.just(new OpencgaEvent("Third event")));
//        OpencgaEventManager.addEvent(OpencgaEventManager.SAMPLE_CREATE, Observable.fromAction(() -> {
//            throw new Exception("asdasd");
//        }));

//        AsyncProcessor<OpencgaEvent> asyncProcessor = AsyncProcessor.create();
//        asyncProcessor.onNext(new OpencgaEvent("Hello"));
//
//        Consumer<OpencgaEvent> consumer = new Consumer<OpencgaEvent>() {
//            @Override
//            public void accept(OpencgaEvent opencgaEvent) throws Throwable {
//                System.out.println("Receive " + opencgaEvent.getMessage());
//            }
//        };
//        asyncProcessor.subscribe(consumer);
//
//        asyncProcessor.onNext(new OpencgaEvent("Bye"));
//        asyncProcessor.subscribe(consumer);
//
//        Flowable.fromAction(() -> {
//
//        })
//        Flowable<OpencgaEvent> myEvent = Flowable.create(emitter -> {
//                    OpencgaEvent event  = new OpencgaEvent("My event");
//                    System.out.println("Emit event " + event.getMessage());
//                    return event;
//                }, BackpressureStrategy.LATEST)
//                .subscribeOn(Schedulers.io());
//        OpencgaEventManager.addEvent(OpencgaEventManager.SAMPLE_CREATE, myEvent);
//
//        Flowable<OpencgaEvent> event = (Flowable<OpencgaEvent>) OpencgaEventManager.getEvent(OpencgaEventManager.SAMPLE_CREATE);
//        event.subscribe(opencgaEvent -> System.out.println(opencgaEvent.getMessage()));
//
//        event.doOnNext(opencgaEvent -> {
//            OpencgaEvent mmm  = new OpencgaEvent("My event");
//            System.out.println("Emit new event " + mmm.getMessage());
//        });
//
//        Thread.sleep(100000);
    }

    @Test
    public void testDefault() {
        Configuration configuration = new Configuration();

        configuration.setLogLevel("INFO");

        configuration.setWorkspace("/opt/opencga/sessions");

        configuration.setAdmin(new Admin());

        Authentication authentication = new Authentication();
        configuration.setAuthentication(authentication);

        configuration.setMonitor(new Monitor());
        configuration.getAnalysis().setExecution(new Execution());

        configuration.setHooks(Collections.singletonMap("organization@project:study", Collections.singletonMap("file",
                Collections.singletonList(
                        new HookConfiguration("name", "~*SV*", HookConfiguration.Stage.CREATE, HookConfiguration.Action.ADD, "tags", "SV")
                ))));

        List<AuthenticationOrigin> authenticationOriginList = new ArrayList<>();
        authenticationOriginList.add(new AuthenticationOrigin());
        Map<String, Object> myMap = new HashMap<>();
        myMap.put("ou", "People");
        authenticationOriginList.add(new AuthenticationOrigin("opencga", AuthenticationOrigin.AuthenticationType.LDAP,
                "ldap://10.10.0.20:389", myMap));
        configuration.getAuthentication().setAuthenticationOrigins(authenticationOriginList);

        Email emailServer = new Email("localhost", "", "", "", "", false);
        configuration.setEmail(emailServer);

        DatabaseCredentials databaseCredentials = new DatabaseCredentials(Arrays.asList("localhost"), "admin", "");
        Catalog catalog = new Catalog();
        catalog.setDatabase(databaseCredentials);
        configuration.setCatalog(catalog);

        Audit audit = new Audit("", 20000000, 100);
        configuration.setAudit(audit);

        ServerConfiguration serverConfiguration = new ServerConfiguration();
        RestServerConfiguration rest = new RestServerConfiguration(1000, 100, 1000);
        GrpcServerConfiguration grpc = new GrpcServerConfiguration(1001);
        serverConfiguration.setGrpc(grpc);
        serverConfiguration.setRest(rest);

        configuration.setServer(serverConfiguration);

//        CellBaseConfiguration cellBaseConfiguration = new CellBaseConfiguration(Arrays.asList("localhost"), "v3",
// new DatabaseCredentials(Arrays.asList("localhost"), "user", "password"));
//        QueryServerConfiguration queryServerConfiguration = new QueryServerConfiguration(61976, Arrays.asList("localhost"));
//
//        catalogConfiguration.setDefaultStorageEngineId("mongodb");
//
//        catalogConfiguration.setCellbase(cellBaseConfiguration);
//        catalogConfiguration.setServer(queryServerConfiguration);
//
//        catalogConfiguration.getStorageEngines().add(storageEngineConfiguration1);
//        catalogConfiguration.getStorageEngines().add(storageEngineConfiguration2);

        try {
            configuration.serialize(new FileOutputStream("/tmp/configuration-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoad() throws Exception {
//        URL url = new URL("http://resources.opencb.org/opencb/opencga/disease-panels/sources.txt");
//        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), Charset.defaultCharset()));
//
//        Set<String> sources = new HashSet<>();
//        String line;
//        while((line = reader.readLine()) != null) {
//            sources.add(line);
//        }
//        System.out.println(sources);
//
//        File file = new File(url.toURI());
//        System.out.println(file.list());

        Configuration configuration = Configuration
                .load(getClass().getResource("/configuration-test.yml").openStream());
        System.out.println("catalogConfiguration = " + configuration);
    }
}