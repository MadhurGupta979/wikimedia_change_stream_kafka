package com.dev.kafkaconsumer.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Table(name = "wikimedia_recent_change")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class WikimediaData {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;

    @Lob
    @Column(name = "data")
    private String wikimediaData;
}
