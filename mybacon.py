import cPickle
import json
import bacon_functions
import time
import gc
from multiprocessing import Pool
import multiprocessing

##ATTEMPT TO PARALLELIZE

"""
shortest_link, find_connection are originally part of bacon_functions
moved here to have __name__=='__main__' : Windows requirement for multiprocessing in python


def shortest_link(params):
       '''
    Return a list of actors (actors are strings)that represents the shortest
    connection between 'actor_name' and Kevin Bacon that can be found in the
    dictionaries: 'actor_dict' and 'movie_dict'.
    '''
    ##same as original function but accepts a single tuple consisting of the below three parameters
    ## Reason : pool.map does not allow you to pass more than one argument to 

    actor_name = params[0];
    actor_dict = params[1];
    movie_dict = params[2];
   

    # Note about the algorithm:
    # Type: Breadth first

    # First, check if the actor's name is 'Kevin Bacon' or if the actor is
    # not present in the 'actor_dict'. If either of them if True
    # then return the empty list.
    if actor_name == 'Kevin Bacon' or not (actor_name in actor_dict):
        return []
    investigated = [actor_name]
    to_investigate = [[actor_name]]
    distance = 0
    # The loop condition checks if the list to_investigate has any remaining
    # elements.
    while to_investigate:
        # Note: As the distance increases the size of each sublist in
        # the nested list 'to_investigate' increases proportionally.
        # The last actor of each sublist is the actor to be investigated
        # The actors which occur before the actor in the sublist simply
        # represent the link from 'actor_name' to that actor.
        # Loop property: The zeroth index changes on every iteration.
        actor_link = to_investigate[0]
        actor = actor_link[distance]

        for movie in actor_dict[actor]:
            for co_star in movie_dict[movie]:
                if not (co_star in investigated):
                    if co_star == "Kevin Bacon":
                        actor_link.append("Kevin Bacon")
                        return actor_link
                    # If the co_star is not present in the list of
                    # investigated actors then make a list containing
                    # the entire link from 'actor_name' to the co_star
                    # and add it to the nested list 'to_investigate'.
                    elif not (co_star in investigated):
                        investigated.append(co_star)
                        full_link = actor_link[:]
                        full_link.append(co_star)
                        to_investigate.append(full_link)
                        # Remove the actor_link (sublist) from the
            # to_investigate (nested list)
        to_investigate.remove(actor_link)
        # Check if all the actor_links of the current distance
        # number is investigated. If that's True then increase
        # the distance by 1.
        if minimum(to_investigate) == distance + 2:
            distance += 1
    return []

def parallel_shortest_link(actor_name,actor_dict,movie_dict):
     #print 'shortest link called with ',actor_name;
     if actor_name == 'Kevin Bacon' or not (actor_name in actor_dict):
        return [];
    ## find all co_stars of the actor
     co_stars = list();     
     links = [actor_name];
     for movie in actor_dict[actor_name]:
         for cs in movie_dict[movie]:
             if cs == 'Kevin Bacon':
                 print 'found';
                 links.append('Kevin Bacon');
                 return links;
             elif cs != actor_name:
                 co_stars.append((cs,actor_dict,movie_dict));
                 print cs;
     #multiprocessing.freeze_support();
     pool = Pool();
     ## do a BFS on all co_stars 
     temp = pool.map(shortest_link,[co_stars]);
     print 'temp is', temp;
     pool.close();
     ##update link list and return
     return links + min(temp,key=len);
            
    
def find_connection(actor_name, actor_dict, movie_dict):
    '''Return a list of (movie, actor) tuples (both elements of type string)
    that represent a shortest connection between 'actor_name' and 'Kevin Bacon'
    that can be found in the actor_dict and movie_dict. Each tuple in the
    returned list has a special property: the actor from the previous tuple and 
    the actor from the current tuple both appeared in the stated movie. If there
    is no connection between 'actor_name' and 'Kevin Bacon', or the 'actor_name'
    is 'Kevin Bacon', the returned list is empty. Note: The actor_dict is the
    inverse of movie_dict.
    '''
    # same as original function
    # The shortest_link function will return the shortest link
    # of actors from 'actor_name' to 'Kevin Bacon'.
    # If there is no connection found or 'actor_name' is
    # 'Kevin Bacon' then it will return an empty list.
    start = time.time();
    link = parallel_shortest_link(actor_name, actor_dict, movie_dict)
    if not link:
        return link
    count = 0
    complete_link = []
    # Loop through the the list 'link' and find the common movie between
    # adjacent actors in the list. Store the common movie and the leading
    # actor as a tuple in the list 'complete_link'.
    for actor in link:
        if count > 0:
            present_actor = link[count]
            previous_actor = link[count - 1]
            common_movie = directly_linked_movie(previous_actor, present_actor, actor_dict, movie_dict)
            movie_actor_tuple = (common_movie, present_actor)
            complete_link.append(movie_actor_tuple)
        count += 1
    print("time taken by bfs :",time.time() - start);
    return complete_link
"""
##END ATTEMPT TO PARALLELIZE

if __name__ == "__main__":
    start = time.time();
    
    #disable garbage collector
    gc.disable();

    # reading json data instead of pickle
    print 'Loading the json data structure of actors to movies ...'
    fp1 = open("my_actors_to_movies.json");
    data  = fp1.read()[1:];
    actors_to_movies = json.loads(data);

    print 'Loading the json data structure of movies to actors' \
          ' ...'
    data= open("my_movies_to_actors.json").read();
    data = data[:795]+data[796:]
    movies_to_actors = json.loads(data);

    
    gc.enable();
    #print "Time taken to load dataset :", time.time()-start;
    
    """
    EXTRA NOTES
    For reasons unknown, json.dump() was writing to the file with an extra {
    This has been manually removed on line 148

    Also, while reading the second JSON usinf json.load() , a [ was being added that led to JSON parse errors
    This also has been manually removed

    These mysterious errors can probably be analysed better with more dataset files, if and when available.
    A better fix can then be proposed rather than manually replacing them
    
    """
    #SAME AS ORIGINAL CODE
    largest = 0
    bacon_number = 0
    while bacon_number != - 1:
        actor_name = raw_input("Please enter an actor (or press return to exit): ")
        actor_name = bacon_functions.capitalize_name(actor_name)
        if actor_name == "Kevin Bacon":
            print "Kevin Bacon has a Bacon Number of 0."
        elif actor_name == "":
            print "Thank you for playing! The largest Bacon Number you found was %d."%(largest)
            bacon_number = -1
        else:
            ##parallel attempt
            #full_link = find_connection(actor_name,actors_to_movies,movies_to_actors)
            full_link = bacon_functions.find_connection(actor_name,actors_to_movies,movies_to_actors)
            # The length of the full link represents the distance for any
            # natural number except 0. For the case of 0, either the actor is
            # either 'Kevin Bacon' (already checked above) or the actor has
            # no connection with 'Kevin Bacon'.
            bacon_number = len(full_link)
            if not bacon_number:
                print "%s has a Bacon Number of Infinity." %(actor_name)
            else:
                print "%s has a Bacon Number of %d." %(actor_name,bacon_number)
                previous_actor = actor_name
                for sublink in full_link:
                    movie,actor = sublink
                    print "%s was in %s with %s."%(previous_actor,movie,actor)
                    previous_actor = actor
        # If the bacon number found for the actor is larger than any bacon
        # number found in the game so far, then assign variable largest
        # to the bacon number.
        if bacon_number > largest:
            largest = bacon_number
        print "\n",
        #SAME AS ORIGINAL CODE
